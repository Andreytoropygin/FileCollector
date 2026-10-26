#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

/**
 * @brief Потокобезопасный коллектор файлов для сборки данных из произвольных сетевых чанков.
 * 
 * Класс обеспечивает эффективное накопление чанков в предварительно выделенном непрерывном
 * буфере памяти с обработкой пересечений, дубликатов и разрывов. Поддерживает многопоточную 
 * запись в один и тот же файл и одновременную работу с множеством независимых файлов.
 */
class FileCollector {
public:
    FileCollector() = default;
    ~FileCollector() = default;

    // Запрет копирования и перемещения для предотвращения состояния гонки с мьютексами.
    FileCollector(const FileCollector&) = delete;
    FileCollector& operator=(const FileCollector&) = delete;

    /**
     * @brief Регистрирует новый файл для сборки и предвыделяет под него непрерывный буфер.
     *  
     * @param fileId Уникальный идентификатор файла.
     * @param fileSize Полный размер файла в байтах.
     * @throws std::invalid_argument Если файл с переданным fileId уже зарегистрирован.
     */
    void CollectFile(uint32_t fileId, size_t fileSize);

    /**
     * @brief Добавляет полученный сетевой чанк к сборке файла.
     * 
     * Анализирует пропуски в файле и дописывает только недостающие части, позволяя
     * нескольким потокам собирать файл одновременно
     * 
     * @param fileId Идентификатор файла.
     * @param pos Позиция чанка внутри целевого файла.
     * @param chunk Вектор данных чанка.
     * @throws std::out_of_range Если fileId не найден в коллекторе.
     */
    void OnNewChunk(uint32_t fileId, size_t pos, std::vector<uint8_t>&& chunk);

    /**
     * @brief Извлекает полностью собранный файл из коллектора.
     * 
     * Использует шаблон блокировки с двойной проверкой для предотвращения блокировки таблицы при частом
     * опросе неготовности и гонки при параллельном извлечении.
     * 
     * @param fileId Идентификатор файла.
     * @return std::optional<std::vector<uint8_t>> Перемещенный вектор с данными файла, 
     *         либо std::nullopt, если файл не найден или еще не собран.
     */
    std::optional<std::vector<uint8_t>> ExtractCompleteFile(uint32_t fileId);

    /**
     * @brief Возвращает уже загруженные непрерывные участки данных (для стриминга).
     * 
     * Позволяет читать данные по мере их поступления, опираясь исключительно на 
     * зафиксированные интервалы, чтобы исключить чтение неинициализированной памяти.
     * 
     * @param fileId Идентификатор файла.
     * @param offset Начальная позиция чтения.
     * @param maxLen Максимальное количество байт для чтения.
     * @return std::vector<uint8_t> Доступный непрерывный фрагмент данных начиная с offset не длиннее maxLen.
     */
    std::vector<uint8_t> GetPartialData(uint32_t fileId, size_t offset = 0, size_t maxLen = SIZE_MAX) const;

    /**
     * @brief Проверяет статус завершения сборки файла.
     * 
     * @param fileId Идентификатор файла.
     * @return true, если файл собран полностью, иначе false.
     */
    bool IsComplete(uint32_t fileId) const;

private:
    /**
     * @brief Описание диапазона для отложенного копирования данных.
     */
    struct CopySpan {
        size_t fileOffset;  // Смещение в целевом буфере файла
        size_t chunkOffset; // Смещение внутри принятого чанка
        size_t length;      // Количество байт для копирования
    };

    /**
     * @brief Контекст отдельного файла, содержащий буфер и метаданные сборки.
     */
    struct FileContext {
        explicit FileContext(size_t size)
            : fileSize(size),
              buffer(size),
              isComplete(size == 0) {}

        const size_t fileSize;
        std::vector<uint8_t> buffer; // Единый непрерывный буфер

        // Разделение интервалов позволяет вынести тяжелый std::memcpy из-под критической секции:
        // - claimedIntervals предотвращает повторное бронирование одних и тех же байтов разными потоками.
        // - committedIntervals отображает читателям байты, которые уже физически записаны в buffer.
        std::map<size_t, size_t> claimedIntervals;
        std::map<size_t, size_t> committedIntervals;

        bool isComplete{false};
        mutable std::shared_mutex mutex; // Мьютекс для синхронизация потоков, работающих над одним файлом
    };

    /**
     * @brief Вспомогательный метод для слияния интервалов.
     * 
     * Автоматически соединяет смежные и пересекающиеся отрезки в единый диапазон.
     */
    static void InsertAndMergeInterval(std::map<size_t, size_t>& intervals, size_t start, size_t end);

    /**
     * @brief Потокобезопасный поиск контекста файла по таблице.
     * 
     * Позволяет параллельно искать разные файлы без взаимных блокировок.
     */
    std::shared_ptr<FileContext> FindContext(uint32_t fileId) const;

    mutable std::shared_mutex map_mutex_; // Мьютекс для независимого доступа к разным fileId.
    std::unordered_map<uint32_t, std::shared_ptr<FileContext>> files_;
};
