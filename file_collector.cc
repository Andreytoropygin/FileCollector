#include "file_collector.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

void FileCollector::CollectFile(uint32_t fileId, size_t fileSize) {
    // Выделение памяти под файл до захвата глобального мьютекса предотвращает задержку других потоков.
    auto ctx = std::make_shared<FileContext>(fileSize);

    std::unique_lock lock(map_mutex_);
    auto [it, inserted] = files_.try_emplace(fileId, std::move(ctx));
    if (!inserted) {
        throw std::invalid_argument("File with this ID is already being collected");
    }
}

std::shared_ptr<FileCollector::FileContext> FileCollector::FindContext(uint32_t fileId) const {
    std::shared_lock lock(map_mutex_);
    auto it = files_.find(fileId);
    return (it != files_.end()) ? it->second : nullptr;
}

void FileCollector::InsertAndMergeInterval(std::map<size_t, size_t>& intervals, size_t start, size_t end) {
    auto it = intervals.upper_bound(start);
    // Проверка предыдущего интервала: возможно перекрытие или стыковка вплотную (prev->second == start).
    if (it != intervals.begin()) {
        auto prev = std::prev(it);
        if (prev->second >= start) {
            it = prev;
        }
    }

    size_t mergedStart = start;
    size_t mergedEnd = end;

    // Соединение смежных и пересекающихся отрезков.
    while (it != intervals.end() && it->first <= mergedEnd) {
        mergedStart = std::min(mergedStart, it->first);
        mergedEnd = std::max(mergedEnd, it->second);
        it = intervals.erase(it);
    }

    intervals.emplace(mergedStart, mergedEnd);
}

void FileCollector::OnNewChunk(uint32_t fileId, size_t pos, std::vector<uint8_t>&& chunk) {
    if (chunk.empty()) {
        return;
    }

    auto ctx = FindContext(fileId);
    if (!ctx) {
        throw std::out_of_range("File ID not found");
    }

    if (pos >= ctx->fileSize) {
        return;
    }
    const size_t end = std::min(pos + chunk.size(), ctx->fileSize);

    std::vector<CopySpan> spansToCopy;

    // Фаза 1: Бронирование только недостающих участков.
    {
        std::unique_lock fileLock(ctx->mutex);
        if (ctx->isComplete) {
            return;
        }

        auto& claimed = ctx->claimedIntervals;
        auto it = claimed.upper_bound(pos);
        if (it != claimed.begin()) {
            auto prev = std::prev(it);
            if (prev->second >= pos) {
                it = prev;
            }
        }

        // Отбрасывание полн дубликата уже принятых или копируемых данных.
        if (it != claimed.end() && it->first <= pos && it->second >= end) {
            return;
        }

        size_t mergedStart = pos;
        size_t mergedEnd = end;
        size_t writeCursor = pos;

        // Вычисление недостающих частей файла, содержащихся в чанке
        while (it != claimed.end() && it->first <= mergedEnd) {
            if (it->first > writeCursor) {
                size_t gapLen = it->first - writeCursor;
                spansToCopy.push_back({writeCursor, writeCursor - pos, gapLen});
            }
            writeCursor = std::max(writeCursor, it->second);
            mergedStart = std::min(mergedStart, it->first);
            mergedEnd = std::max(mergedEnd, it->second);
            it = claimed.erase(it);
        }

        if (writeCursor < end) {
            size_t tailLen = end - writeCursor;
            spansToCopy.push_back({writeCursor, writeCursor - pos, tailLen});
        }

        claimed.emplace(mergedStart, mergedEnd);
    }

    // Фаза 2: Копирование данных без удержания мьютекса.
    // Безопасно, так как буфер не реаллоцируется, а диапазоны в spansToCopy эксклюзивно забронированы.
    for (const auto& span : spansToCopy) {
        std::memcpy(
            ctx->buffer.data() + span.fileOffset,
            chunk.data() + span.chunkOffset,
            span.length
        );
    }

    // Фаза 3: Коммит записанных диапазонов.
    {
        std::unique_lock fileLock(ctx->mutex);
        for (const auto& span : spansToCopy) {
            InsertAndMergeInterval(
                ctx->committedIntervals,
                span.fileOffset,
                span.fileOffset + span.length
            );
        }

        if (ctx->committedIntervals.size() == 1) {
            const auto& first = *ctx->committedIntervals.begin();
            if (first.first == 0 && first.second == ctx->fileSize) {
                ctx->isComplete = true;
            }
        }
    }
}

std::optional<std::vector<uint8_t>> FileCollector::ExtractCompleteFile(uint32_t fileId) {
    auto ctx = FindContext(fileId);
    if (!ctx) {
        return std::nullopt;
    }

    // Первичная проверка готовности предотвращает блокировку работы с другими файлами при polling.
    {
        std::shared_lock fileLock(ctx->mutex);
        if (!ctx->isComplete) {
            return std::nullopt;
        }
    }

    std::shared_ptr<FileContext> extractedCtx;
    {
        std::unique_lock mapLock(map_mutex_);
        auto it = files_.find(fileId);
        if (it == files_.end()) {
            return std::nullopt; // Защита от гонки при параллельном извлечении.
        }
        extractedCtx = std::move(it->second);
        files_.erase(it);
    }

    // Ожидание завершения возможных запоздалых потоков, получивших shared_ptr до удаления из files_.
    std::unique_lock fileLock(extractedCtx->mutex);
    return std::move(extractedCtx->buffer);
}

std::vector<uint8_t> FileCollector::GetPartialData(uint32_t fileId, size_t offset, size_t maxLen) const {
    if (maxLen == 0) {
        return {};
    }

    auto ctx = FindContext(fileId);
    if (!ctx) {
        return {};
    }

    std::shared_lock fileLock(ctx->mutex);
    if (offset >= ctx->fileSize) {
        return {};
    }

    if (ctx->isComplete) {
        size_t copyLen = std::min(maxLen, ctx->fileSize - offset);
        return std::vector<uint8_t>(
            ctx->buffer.begin() + offset,
            ctx->buffer.begin() + offset + copyLen
        );
    }

    // Использование исключительно committedIntervals исключает чтение неинициализированной памяти,
    // в которую выполняется асинхронный memcpy.
    auto it = ctx->committedIntervals.upper_bound(offset);
    if (it == ctx->committedIntervals.begin()) {
        return {};
    }

    auto covering = std::prev(it);
    if (covering->second <= offset) {
        return {};
    }

    size_t availableBytes = covering->second - offset;
    size_t copyLen = std::min(maxLen, availableBytes);

    return std::vector<uint8_t>(
        ctx->buffer.begin() + offset,
        ctx->buffer.begin() + offset + copyLen
    );
}

bool FileCollector::IsComplete(uint32_t fileId) const {
    auto ctx = FindContext(fileId);
    if (!ctx) {
        return false;
    }

    std::shared_lock fileLock(ctx->mutex);
    return ctx->isComplete;
}
