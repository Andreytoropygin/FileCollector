#include "file_collector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

/**
 * @brief Направлен на проверку базовой бизнес-логики на точном примере из условия задачи.
 * 
 * Верифицирует корректность сборки файла размером 10 байт при получении трех 
 * пересекающихся чанков ([0..4), [2..6), [6..10)), промежуточные статусы IsComplete,
 * частичное чтение префикса и финальное извлечение данных.
 */
TEST(FileCollectorTest, ExampleFromPrompt) {
    FileCollector collector;
    const uint32_t fileId = 1;
    collector.CollectFile(fileId, 10);

    collector.OnNewChunk(fileId, 0, {'A', 'B', 'C', 'D'});
    collector.OnNewChunk(fileId, 2, {'C', 'D', 'E', 'F'});
    EXPECT_FALSE(collector.IsComplete(fileId));

    std::vector<uint8_t> expectedPartial = {'A', 'B', 'C', 'D', 'E', 'F'};
    EXPECT_EQ(collector.GetPartialData(fileId, 0), expectedPartial);

    collector.OnNewChunk(fileId, 6, {'G', 'H', 'I', 'J'});
    EXPECT_TRUE(collector.IsComplete(fileId));

    auto result = collector.ExtractCompleteFile(fileId);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, (std::vector<uint8_t>{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'}));

    // Проверка освобождения ресурсов после извлечения.
    EXPECT_EQ(collector.ExtractCompleteFile(fileId), std::nullopt);
    EXPECT_FALSE(collector.IsComplete(fileId));
}

/**
 * @brief Направлен на проверку поведения класса при хаотичном порядке чанков и образовании разрывов.
 * 
 * Проверяет корректность работы GetPartialData при наличии пропусков в данных (чтение прерывается 
 * на первом разрыве), а также способность длинного перекрывающего чанка объединить несколько 
 * изолированных интервалов в единый массив.
 */
TEST(FileCollectorTest, OutOfOrderAndBridgingIntervals) {
    FileCollector collector;
    const uint32_t fileId = 42;
    collector.CollectFile(fileId, 12);

    collector.OnNewChunk(fileId, 8, {8, 9, 10, 11});
    collector.OnNewChunk(fileId, 0, {0, 1, 2});
    collector.OnNewChunk(fileId, 4, {4, 5, 6});

    // Чтение прерывается на первой незаполненной области.
    EXPECT_EQ(collector.GetPartialData(fileId, 0), (std::vector<uint8_t>{0, 1, 2}));
    EXPECT_TRUE(collector.GetPartialData(fileId, 3).empty());

    // Объединение трех изолированных интервалов единым перекрывающим чанком.
    collector.OnNewChunk(fileId, 2, {2, 3, 4, 5, 6, 7, 8});
    EXPECT_TRUE(collector.IsComplete(fileId));

    auto file = collector.ExtractCompleteFile(fileId);
    ASSERT_TRUE(file.has_value());
    std::vector<uint8_t> expected(12);
    std::iota(expected.begin(), expected.end(), 0);
    EXPECT_EQ(*file, expected);
}

/**
 * @brief Направлен на проверку защиты от дубликатов и избыточных данных.
 * 
 * Тестирует игнорирование полных дубликатов чанков, чанков, полностью лежащих внутри 
 * уже собранных областей, пустых векторов, а также поведение при попытке отправки 
 * чанка после завершения сборки файла.
 */
TEST(FileCollectorTest, DuplicatesAndSubsumedChunks) {
    FileCollector collector;
    const uint32_t fileId = 7;
    collector.CollectFile(fileId, 6);

    collector.OnNewChunk(fileId, 0, {10, 20, 30, 40});
    collector.OnNewChunk(fileId, 0, {10, 20, 30, 40});
    collector.OnNewChunk(fileId, 1, {20, 30});
    collector.OnNewChunk(fileId, 2, {});

    EXPECT_FALSE(collector.IsComplete(fileId));

    collector.OnNewChunk(fileId, 4, {50, 60});
    EXPECT_TRUE(collector.IsComplete(fileId));

    // Игнорирование дубликата после завершения сборки.
    collector.OnNewChunk(fileId, 0, {10, 20});

    auto file = collector.ExtractCompleteFile(fileId);
    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(*file, (std::vector<uint8_t>{10, 20, 30, 40, 50, 60}));
}

/**
 * @brief Направлен на проверку обработки краевых условий и валидации параметров.
 * 
 * Верифицирует корректность работы с файлами нулевого размера, генерацию исключений 
 * при повторной регистрации ID или обращении к несуществующему файлу, а также обрезку 
 * чанков, выходящих за правую границу fileSize.
 */
TEST(FileCollectorTest, EdgeCasesAndExceptions) {
    FileCollector collector;

    collector.CollectFile(0, 0);
    EXPECT_TRUE(collector.IsComplete(0));
    auto emptyFile = collector.ExtractCompleteFile(0);
    ASSERT_TRUE(emptyFile.has_value());
    EXPECT_TRUE(emptyFile->empty());

    collector.CollectFile(10, 5);
    EXPECT_THROW(collector.CollectFile(10, 5), std::invalid_argument);
    EXPECT_THROW(collector.OnNewChunk(99, 0, {1, 2, 3}), std::out_of_range);

    // Данные за пределами размера файла отсекаются для предотвращения выхода за границы буфера.
    collector.OnNewChunk(10, 3, {3, 4, 99, 100, 101});
    collector.OnNewChunk(10, 100, {1, 2});
    collector.OnNewChunk(10, 0, {0, 1, 2});

    auto file = collector.ExtractCompleteFile(10);
    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(*file, (std::vector<uint8_t>{0, 1, 2, 3, 4}));
}

/**
 * @brief Направлен на стресс-тестирование многопоточной записи в один большой файл.
 * 
 * Симулирует загрузку файла объемом 256 КБ из 8 потоков одновременно. Чанки 
 * генерируются с сильными пересечениями и перемешиваются в случайном порядке для 
 * проверки отсутствия гонок данных (Data Races) при параллельном копировании.
 */
TEST(FileCollectorTest, ConcurrentSingleFileAssembly) {
    FileCollector collector;
    const uint32_t fileId = 100;
    const size_t fileSize = 256 * 1024;
    collector.CollectFile(fileId, fileSize);

    std::vector<uint8_t> expectedData(fileSize);
    for (size_t i = 0; i < fileSize; ++i) {
        expectedData[i] = static_cast<uint8_t>((i * 31 + 17) & 0xFF);
    }

    struct ChunkTask {
        size_t pos;
        std::vector<uint8_t> data;
    };

    std::vector<ChunkTask> tasks;
    const size_t step = 100;
    const size_t chunkLen = 160;
    for (size_t pos = 0; pos < fileSize; pos += step) {
        size_t len = std::min(chunkLen, fileSize - pos);
        tasks.push_back({
            pos,
            std::vector<uint8_t>(expectedData.begin() + pos, expectedData.begin() + pos + len)
        });
    }

    std::mt19937 rng(42);
    std::shuffle(tasks.begin(), tasks.end(), rng);

    const size_t numThreads = 8;
    std::atomic<size_t> taskIdx{0};
    std::vector<std::thread> workers;

    for (size_t t = 0; t < numThreads; ++t) {
        workers.emplace_back([&]() {
            while (true) {
                size_t i = taskIdx.fetch_add(1, std::memory_order_relaxed);
                if (i >= tasks.size()) {
                    break;
                }
                auto chunkCopy = tasks[i].data;
                collector.OnNewChunk(fileId, tasks[i].pos, std::move(chunkCopy));
            }
        });
    }

    for (auto& w : workers) {
        w.join();
    }

    auto assembled = collector.ExtractCompleteFile(fileId);
    ASSERT_TRUE(assembled.has_value());
    EXPECT_EQ(*assembled, expectedData);
}

/**
 * @brief Направлен на стресс-тестирование одновременной работы с множеством файлов и конкурентным извлечением.
 * 
 * Проверяет независимость работы потоков с разными файлами, параллельное чтение частичных 
 * данных (GetPartialData) и состояние гонки в ExtractCompleteFile при одновременной 
 * попытке нескольких потоков-экстракторов забрать готовые файлы.
 */
TEST(FileCollectorTest, ConcurrentMultipleFilesAndExtractionRace) {
    FileCollector collector;
    const uint32_t numFiles = 30;
    const size_t fileSize = 4096;
    const int numThreads = 4;
    size_t chunkSize = fileSize / numThreads + 8;

    for (uint32_t id = 0; id < numFiles; ++id) {
        collector.CollectFile(id, fileSize);
    }

    std::vector<std::thread> writers;
    for (int t = 0; t < numThreads; ++t) {
        writers.emplace_back([&, t]() {
            for (uint32_t id = 0; id < numFiles; ++id) {
                size_t pos = (t % numThreads) * (fileSize / numThreads);
                size_t len = std::min(chunkSize, fileSize - pos);
                std::vector<uint8_t> chunk(len, static_cast<uint8_t>(id));
                collector.OnNewChunk(id, pos, std::move(chunk));
            }
        });
    }

    std::atomic<uint32_t> extractedCount{0};
    std::vector<std::thread> extractors;
    for (int r = 0; r < 4; ++r) {
        extractors.emplace_back([&]() {
            while (extractedCount.load(std::memory_order_relaxed) < numFiles) {
                for (uint32_t id = 0; id < numFiles; ++id) {
                    (void)collector.GetPartialData(id, 0, 128);

                    auto res = collector.ExtractCompleteFile(id);
                    if (res.has_value()) {
                        EXPECT_EQ(*res, std::vector<uint8_t>(fileSize, static_cast<uint8_t>(id)));
                        extractedCount.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                std::this_thread::yield();
            }
        });
    }

    for (auto& w : writers) {
        w.join();
    }
    for (auto& e : extractors) {
        e.join();
    }

    EXPECT_EQ(extractedCount.load(), numFiles);
}
