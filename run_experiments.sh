#!/bin/bash

# Script de automatización de experimentos para comparación de políticas de caché
# Ejecuta múltiples configuraciones y guarda las métricas resultantes

echo "=== Iniciando Experimentos de Caché ==="
echo "Este script ejecutará múltiples configuraciones y guardará los resultados"
echo ""

# Verificar y crear .env si no existe
if [ ! -f .env ]; then
    echo "⚠️  Archivo .env no encontrado. Creando desde .env.example..."
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "✓ Archivo .env creado."
        echo "⚠️  IMPORTANTE: Debes configurar GEMINI_API_KEY en .env antes de continuar."
        echo ""
        read -p "¿Deseas editar .env ahora? (s/n): " edit_env
        if [ "$edit_env" = "s" ] || [ "$edit_env" = "S" ]; then
            ${EDITOR:-nano} .env
        else
            echo "Por favor, edita .env manualmente y ejecuta el script nuevamente."
            exit 1
        fi
    else
        echo "❌ Error: .env.example no encontrado."
        exit 1
    fi
fi

# Verificar que GEMINI_API_KEY esté configurado
if grep -q "GEMINI_API_KEY=your_api_key_here" .env || grep -q "GEMINI_API_KEY=$" .env; then
    echo "❌ Error: GEMINI_API_KEY no está configurado en .env"
    echo "Por favor, edita .env y configura tu API key de Google Gemini."
    exit 1
fi

echo "✓ Configuración .env verificada"
echo ""

# Crear directorio para resultados
RESULTS_DIR="resultados_experimentos"
mkdir -p "$RESULTS_DIR"

# Función para ejecutar un experimento
run_experiment() {
    local POLICY=$1
    local SIZE=$2
    local TTL=$3
    local SLEEP=$4
    local DURATION=$5
    
    local EXPERIMENT_NAME="${POLICY}_size${SIZE}_ttl${TTL}_sleep${SLEEP}"
    echo ""
    echo "========================================"
    echo "Ejecutando: $EXPERIMENT_NAME"
    echo "========================================"
    
    # Modificar .env
    sed -i "s/^CACHE_POLICY=.*/CACHE_POLICY=$POLICY/" .env
    sed -i "s/^CACHE_SIZE=.*/CACHE_SIZE=$SIZE/" .env
    sed -i "s/^CACHE_TTL=.*/CACHE_TTL=$TTL/" .env
    sed -i "s/^SLEEP_TIME=.*/SLEEP_TIME=$SLEEP/" .env
    
    # Levantar servicios
    echo "Iniciando contenedores..."
    docker-compose down -v > /dev/null 2>&1
    docker-compose up -d --build > /dev/null 2>&1
    
    # Esperar que los servicios estén listos
    echo "Esperando inicialización (30s)..."
    sleep 30
    
    # Ejecutar por el tiempo especificado
    echo "Recolectando datos ($DURATION segundos)..."
    sleep $DURATION
    
    # Obtener estadísticas
    echo "Guardando resultados..."
    curl -s http://localhost:8001/stats > "$RESULTS_DIR/${EXPERIMENT_NAME}.json"
    
    # Guardar conteo de respuestas en BD
    docker exec db psql -U user -d yahoo_db -c "SELECT COUNT(*) as total_responses FROM responses;" -t > "$RESULTS_DIR/${EXPERIMENT_NAME}_db_count.txt"
    
    echo "✓ Experimento completado: $EXPERIMENT_NAME"
}

# ============================================================
# EXPERIMENTO 1: Comparación de Políticas (tamaño fijo)
# ============================================================
echo ""
echo "### EXPERIMENTO 1: Comparación de Políticas ###"
DURATION=300  # 5 minutos por política

run_experiment "LRU" 1500 0 1.5 $DURATION
run_experiment "FIFO" 1500 0 1.5 $DURATION
run_experiment "LFU" 1500 0 1.5 $DURATION

# ============================================================
# EXPERIMENTO 2: Impacto del Tamaño de Caché (política LRU)
# ============================================================
echo ""
echo "### EXPERIMENTO 2: Impacto del Tamaño ###"
DURATION=300

run_experiment "LRU" 500 0 1.5 $DURATION
run_experiment "LRU" 1000 0 1.5 $DURATION
run_experiment "LRU" 2000 0 1.5 $DURATION

# ============================================================
# EXPERIMENTO 3: Distribuciones de Tráfico (LRU, tamaño 1500)
# ============================================================
echo ""
echo "### EXPERIMENTO 3: Distribuciones de Tráfico ###"
DURATION=300

run_experiment "LRU" 1500 0 0.5 $DURATION   # Alta frecuencia
run_experiment "LRU" 1500 0 3.0 $DURATION   # Baja frecuencia

# ============================================================
# EXPERIMENTO 4: Impacto del TTL (LRU, tamaño 1500)
# ============================================================
echo ""
echo "### EXPERIMENTO 4: Impacto del TTL ###"
DURATION=400  # Más tiempo para observar expiraciones

run_experiment "LRU" 1500 60 1.5 $DURATION    # TTL 1 minuto
run_experiment "LRU" 1500 300 1.5 $DURATION   # TTL 5 minutos
run_experiment "LRU" 1500 600 1.5 $DURATION   # TTL 10 minutos

# ============================================================
# Finalizar
# ============================================================
echo ""
echo "========================================"
echo "✓ Todos los experimentos completados"
echo "========================================"
echo ""
echo "Resultados guardados en: $RESULTS_DIR/"
echo ""
echo "Para analizar los resultados, ejecuta:"
echo "  python3 analyze_results.py"
echo ""

# Detener contenedores
docker-compose down
