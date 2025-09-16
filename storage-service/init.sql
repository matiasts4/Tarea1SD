-- Usamos CREATE TABLE IF NOT EXISTS para evitar errores si la tabla ya fue creada.
CREATE TABLE IF NOT EXISTS responses (
    id SERIAL PRIMARY KEY,
    question TEXT NOT NULL UNIQUE,
    original_answer TEXT,
    llm_answer TEXT,
    score FLOAT,
    hit_count INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Creamos un índice en la columna 'question' para acelerar las búsquedas.
CREATE INDEX IF NOT EXISTS idx_question ON responses (question);
