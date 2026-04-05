-- Контрольная работа 3
-- Тема: Разработка модели данных до 3НФ для OLTP
--       и реализация полнотекстового поиска в PostgreSQL
--
-- Предметная область: учебные курсы
--
-- Что делает этот файл:
-- 1) Создает таблицы categories, instructors, courses
-- 2) Подключает расширение pg_trgm
-- 3) Создает индексы для JOIN / WHERE / ORDER BY / полнотекстового поиска
-- 4) Заполняет таблицы тестовыми данными
-- 5) Выполняет ANALYZE для актуальной статистики планировщика
-- 6) Содержит примеры SELECT и EXPLAIN ANALYZE для демонстрации
-- ===========================================================================

-- ============================================================================
-- 0. Подготовка
-- ============================================================================

-- Расширение pg_trgm нужно для триграммного поиска по частичному совпадению.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Удаляем таблицы, если они уже существуют.
-- Это делает файл воспроизводимым: можно запускать повторно.
DROP TABLE IF EXISTS courses CASCADE;
DROP TABLE IF EXISTS instructors CASCADE;
DROP TABLE IF EXISTS categories CASCADE;


-- ============================================================================
-- 1. Создание таблиц
-- ============================================================================

-- --------------------------------------------------------------------------
-- Таблица категорий
-- --------------------------------------------------------------------------
-- Хранит только данные о категории курса.
-- Все неключевые атрибуты зависят только от category_id.
CREATE TABLE categories (
    category_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);


-- --------------------------------------------------------------------------
-- Таблица преподавателей
-- --------------------------------------------------------------------------
-- Хранит только данные о преподавателе.
-- email сделан уникальным, так как один email не должен повторяться.
CREATE TABLE instructors (
    instructor_id BIGSERIAL PRIMARY KEY,
    full_name VARCHAR(150) NOT NULL,
    email VARCHAR(150) NOT NULL UNIQUE
);


-- --------------------------------------------------------------------------
-- Таблица курсов
-- --------------------------------------------------------------------------
-- Хранит основные данные о курсе.
-- Внешние ключи:
--   category_id   -> categories(category_id)
--   instructor_id -> instructors(instructor_id)
--
-- Два текстовых атрибута для поиска:
--   title
--   description
--
-- Это значит, что совпадение в названии курса важнее,
-- чем совпадение только в описании.
CREATE TABLE courses (
    course_id BIGSERIAL PRIMARY KEY,
    category_id BIGINT NOT NULL REFERENCES categories(category_id),
    instructor_id BIGINT NOT NULL REFERENCES instructors(instructor_id),
    title VARCHAR(200) NOT NULL,
    description TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    duration_hours INT NOT NULL CHECK (duration_hours > 0),
    start_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    search_vector tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('russian', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('russian', coalesce(description, '')), 'B')
    ) STORED
);


-- ============================================================================
-- 2. Индексы
-- ============================================================================
-- Индексы для внешних ключей
-- --------------------------------------------------------------------------
-- Эти индексы помогают JOIN и фильтрации по category_id / instructor_id.
CREATE INDEX idx_courses_category_id ON courses(category_id);
CREATE INDEX idx_courses_instructor_id ON courses(instructor_id);

-- Индекс для сортировки / фильтрации по дате старта.
CREATE INDEX idx_courses_start_date ON courses(start_date);


-- --------------------------------------------------------------------------
-- GIN-индекс для полнотекстового поиска
-- --------------------------------------------------------------------------
-- Используется запросами вида:
--   WHERE search_vector @@ plainto_tsquery('russian', '...')
CREATE INDEX idx_courses_search_vector
ON courses
USING GIN (search_vector);


-- --------------------------------------------------------------------------
-- GIN trigram индексы для поиска по частичному совпадению
-- --------------------------------------------------------------------------
-- Нужны для LIKE / ILIKE / similarity по части слова.
-- lower(...) используется для регистронезависимого поиска.
CREATE INDEX idx_courses_title_trgm
ON courses
USING GIN (lower(title) gin_trgm_ops);

CREATE INDEX idx_courses_description_trgm
ON courses
USING GIN (lower(description) gin_trgm_ops);


-- ============================================================================
-- 3. Наполнение справочников тестовыми данными
-- ============================================================================

INSERT INTO categories (name) VALUES
('Базы данных'),
('Веб-разработка'),
('Тестирование'),
('Аналитика данных'),
('Программирование'),
('DevOps');

INSERT INTO instructors (full_name, email) VALUES
('Иван Петров', 'petrov@example.com'),
('Мария Соколова', 'sokolova@example.com'),
('Алексей Смирнов', 'smirnov@example.com'),
('Ольга Васильева', 'vasilieva@example.com'),
('Дмитрий Козлов', 'kozlov@example.com'),
('Елена Морозова', 'morozova@example.com'),
('Николай Орлов', 'orlov@example.com'),
('Анна Федорова', 'fedorova@example.com');


-- ============================================================================
-- 4. Наполнение courses реалистичными тестовыми данными
-- ============================================================================

-- Для демонстрации EXPLAIN ANALYZE желательно не 10 и не 20 строк,
-- а достаточно много записей. Иначе PostgreSQL может выбрать Seq Scan,
-- даже если индексы созданы корректно.
--
-- Здесь мы создаем 2500 курсов на основе шаблонов.
-- Этого достаточно для учебной демонстрации.
INSERT INTO courses (
    category_id,
    instructor_id,
    title,
    description,
    price,
    duration_hours,
    start_date
)
SELECT
    (gs % 6) + 1,
    (gs % 8) + 1,
    CASE gs % 8
        WHEN 0 THEN format('Практический курс SQL и PostgreSQL, поток %s', gs)
        WHEN 1 THEN format('Веб-разработка на Python и Django, поток %s', gs)
        WHEN 2 THEN format('Тестирование веб-приложений, поток %s', gs)
        WHEN 3 THEN format('Анализ данных в Excel и SQL, поток %s', gs)
        WHEN 4 THEN format('Java для начинающих, поток %s', gs)
        WHEN 5 THEN format('Администрирование Linux и Docker, поток %s', gs)
        WHEN 6 THEN format('Frontend: HTML, CSS и JavaScript, поток %s', gs)
        ELSE format('DevOps и CI/CD для начинающих, поток %s', gs)
    END,
    CASE gs % 8
        WHEN 0 THEN 'Курс по написанию SQL-запросов, работе с PostgreSQL, индексами, нормализацией данных и оптимизации поиска.'
        WHEN 1 THEN 'Курс по созданию веб-приложений на Python и Django, работе с шаблонами, ORM, формами и базами данных.'
        WHEN 2 THEN 'Практика ручного тестирования, составление тест-кейсов, поиск дефектов и проверка веб-приложений.'
        WHEN 3 THEN 'Обучение анализу данных, работе с Excel, SQL, сводными таблицами и подготовке отчетов.'
        WHEN 4 THEN 'Основы Java, объектно-ориентированное программирование, коллекции, исключения, файлы и базовая многопоточность.'
        WHEN 5 THEN 'Основы Linux, работа с командной строкой, процессами, файлами, Docker-контейнерами и системными журналами.'
        WHEN 6 THEN 'Верстка интерфейсов с использованием HTML, CSS, JavaScript, адаптивной сетки и базовой анимации.'
        ELSE 'Введение в DevOps: Git, CI/CD, Docker, автоматизация сборки, деплой и мониторинг сервисов.'
    END,
    12000 + (gs % 9) * 1500,
    24 + (gs % 5) * 8,
    CURRENT_DATE + (gs % 90)
FROM generate_series(1, 2500) AS gs;


-- ============================================================================
-- 5. Обновление статистики планировщика
-- ============================================================================
ANALYZE;


-- ============================================================================
-- 6. Примеры поисковых запросов для демонстрации
-- ============================================================================
-- 6.1. Полнотекстовый поиск с учетом морфологии русского языка
-- --------------------------------------------------------------------------
-- В запросе ниже ищем по словам "тестирование приложений".
-- Используется russian словарь, поэтому учитываются словоформы.
-- Важно: здесь должен использоваться GIN-индекс idx_courses_search_vector.
EXPLAIN ANALYZE
SELECT
    c.course_id,
    c.title,
    cat.name AS category_name,
    i.full_name AS instructor_name,
    ts_rank(c.search_vector, plainto_tsquery('russian', 'тестирование приложений')) AS rank
FROM courses c
JOIN categories cat ON cat.category_id = c.category_id
JOIN instructors i ON i.instructor_id = c.instructor_id
WHERE c.search_vector @@ plainto_tsquery('russian', 'тестирование приложений')
ORDER BY rank DESC, c.course_id
LIMIT 10;


SELECT
    c.course_id,
    c.title,
    cat.name AS category_name,
    i.full_name AS instructor_name,
    ts_rank(c.search_vector, plainto_tsquery('russian', 'тестирование приложений')) AS rank
FROM courses c
JOIN categories cat ON cat.category_id = c.category_id
JOIN instructors i ON i.instructor_id = c.instructor_id
WHERE c.search_vector @@ plainto_tsquery('russian', 'тестирование приложений')
ORDER BY rank DESC, c.course_id
LIMIT 10;


-- --------------------------------------------------------------------------
-- 6.2. Полнотекстовый поиск по другой теме
-- --------------------------------------------------------------------------
-- Ищем курсы по базам данных и PostgreSQL.
EXPLAIN ANALYZE
SELECT
    c.course_id,
    c.title,
    ts_rank(c.search_vector, plainto_tsquery('russian', 'postgresql база данных')) AS rank
FROM courses c
WHERE c.search_vector @@ plainto_tsquery('russian', 'postgresql база данных')
ORDER BY rank DESC, c.course_id
LIMIT 10;

SELECT
    c.course_id,
    c.title,
    ts_rank(c.search_vector, plainto_tsquery('russian', 'postgresql база данных')) AS rank
FROM courses c
WHERE c.search_vector @@ plainto_tsquery('russian', 'postgresql база данных')
ORDER BY rank DESC, c.course_id
LIMIT 10;


-- --------------------------------------------------------------------------
-- 6.3. Триграммный поиск по частичному совпадению в title
-- --------------------------------------------------------------------------
-- Оператор % из расширения pg_trgm позволяет искать строки,
-- похожие на заданное слово.
-- similarity(...) показывает степень сходства строки с образцом.
-- Для ускорения должен использоваться trigram индекс idx_courses_title_trgm.
EXPLAIN ANALYZE
SELECT
    c.course_id,
    c.title,
    similarity(lower(c.title), 'аналитика') AS sim
FROM courses c
WHERE lower(c.title) % 'аналитика'
ORDER BY sim DESC, c.course_id
LIMIT 10;

SELECT
    c.course_id,
    c.title,
    similarity(lower(c.title), 'аналитика') AS sim
FROM courses c
WHERE lower(c.title) % 'аналитика'
ORDER BY sim DESC, c.course_id
LIMIT 10;


-- --------------------------------------------------------------------------
-- 6.4. Триграммный поиск по частичному совпадению в title
-- --------------------------------------------------------------------------
-- Ищем похожие названия курсов по слову docker.
-- Для ускорения должен использоваться trigram индекс idx_courses_title_trgm.
EXPLAIN ANALYZE
SELECT
    c.course_id,
    c.title,
    similarity(lower(c.title), 'docker') AS sim
FROM courses c
WHERE lower(c.title) % 'docker'
ORDER BY sim DESC, c.course_id
LIMIT 10;

SELECT
    c.course_id,
    c.title,
    similarity(lower(c.title), 'docker') AS sim
FROM courses c
WHERE lower(c.title) % 'docker'
ORDER BY sim DESC, c.course_id
LIMIT 10;


-- --------------------------------------------------------------------------
-- 6.5. Пример обычного JOIN-запроса с фильтрацией и сортировкой
-- --------------------------------------------------------------------------
-- Этот запрос полезен для демонстрации того,
-- что схема подходит под OLTP и имеет индексы для JOIN / WHERE / ORDER BY.
EXPLAIN ANALYZE
SELECT
    c.course_id,
    c.title,
    c.start_date,
    cat.name AS category_name,
    i.full_name AS instructor_name
FROM courses c
JOIN categories cat ON cat.category_id = c.category_id
JOIN instructors i ON i.instructor_id = c.instructor_id
WHERE c.category_id = 1
ORDER BY c.start_date, c.course_id
LIMIT 15;

SELECT
    c.course_id,
    c.title,
    c.start_date,
    cat.name AS category_name,
    i.full_name AS instructor_name
FROM courses c
JOIN categories cat ON cat.category_id = c.category_id
JOIN instructors i ON i.instructor_id = c.instructor_id
WHERE c.category_id = 1
ORDER BY c.start_date, c.course_id
LIMIT 15;


-- ============================================================================
-- 7.3НФ
-- ============================================================================

-- categories:
--   category_id -> name
--   Все атрибуты зависят только от первичного ключа.
--
-- instructors:
--   instructor_id -> full_name, email
--   Все атрибуты зависят только от первичного ключа.
--
-- courses:
--   course_id -> category_id, instructor_id, title, description,
--                price, duration_hours, start_date, created_at, search_vector
--   Все неключевые атрибуты зависят только от course_id.
--
-- Транзитивных зависимостей нет, потому что:
--   - название категории не хранится в courses,
--   - данные преподавателя не хранятся в courses,
--   - они вынесены в отдельные таблицы categories и instructors.

