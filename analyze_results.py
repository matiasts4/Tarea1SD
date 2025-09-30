#!/usr/bin/env python3
"""
Script para analizar y comparar resultados de experimentos de caché.
Genera tablas y gráficos comparativos.
"""

import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configurar estilo de gráficos
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

def load_experiments(results_dir="resultados_experimentos"):
    """Carga todos los archivos JSON de resultados."""
    experiments = []
    
    for filename in os.listdir(results_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(results_dir, filename)
            with open(filepath, 'r') as f:
                data = json.load(f)
                
            # Extraer parámetros del nombre del archivo
            params = filename.replace('.json', '').split('_')
            data['policy'] = params[0]
            data['size'] = int(params[1].replace('size', ''))
            data['ttl'] = int(params[2].replace('ttl', ''))
            data['sleep'] = float(params[3].replace('sleep', ''))
            data['experiment'] = filename.replace('.json', '')
            
            experiments.append(data)
    
    return pd.DataFrame(experiments)

def analyze_policies(df):
    """Análisis comparativo de políticas de caché."""
    print("\n" + "="*70)
    print("EXPERIMENTO 1: COMPARACIÓN DE POLÍTICAS DE CACHÉ")
    print("="*70)
    
    # Filtrar: mismo tamaño, sin TTL, mismo tráfico
    policy_data = df[(df['size'] == 1500) & (df['ttl'] == 0) & (df['sleep'] == 1.5)]
    
    if len(policy_data) > 0:
        comparison = policy_data[['policy', 'hits', 'misses', 'evictions', 'hit_rate']].sort_values('hit_rate', ascending=False)
        print("\nResultados:")
        print(comparison.to_string(index=False))
        
        # Calcular mejora
        if len(comparison) > 1:
            best = comparison.iloc[0]
            worst = comparison.iloc[-1]
            improvement = ((best['hit_rate'] - worst['hit_rate']) / worst['hit_rate']) * 100
            print(f"\n✓ Mejor política: {best['policy']} con {best['hit_rate']:.2f}% hit rate")
            print(f"✓ Mejora respecto a la peor: {improvement:.2f}%")
        
        # Generar gráfico
        plt.figure(figsize=(10, 6))
        plt.bar(comparison['policy'], comparison['hit_rate'], color=['#2ecc71', '#3498db', '#e74c3c'])
        plt.xlabel('Política de Caché')
        plt.ylabel('Hit Rate (%)')
        plt.title('Comparación de Hit Rate por Política de Caché')
        plt.ylim(0, 100)
        for i, v in enumerate(comparison['hit_rate']):
            plt.text(i, v + 2, f'{v:.1f}%', ha='center', fontweight='bold')
        plt.tight_layout()
        plt.savefig('grafico_politicas.png', dpi=300, bbox_inches='tight')
        print("\n✓ Gráfico guardado: grafico_politicas.png")

def analyze_cache_size(df):
    """Análisis del impacto del tamaño de caché."""
    print("\n" + "="*70)
    print("EXPERIMENTO 2: IMPACTO DEL TAMAÑO DE CACHÉ")
    print("="*70)
    
    # Filtrar: política LRU, sin TTL, mismo tráfico
    size_data = df[(df['policy'] == 'LRU') & (df['ttl'] == 0) & (df['sleep'] == 1.5)]
    
    if len(size_data) > 0:
        comparison = size_data[['size', 'hits', 'misses', 'evictions', 'hit_rate']].sort_values('size')
        print("\nResultados:")
        print(comparison.to_string(index=False))
        
        # Calcular eficiencia (hit_rate por unidad de tamaño)
        comparison['efficiency'] = comparison['hit_rate'] / comparison['size'] * 1000
        print(f"\n✓ Eficiencia (hit_rate por 1000 unidades de tamaño):")
        for _, row in comparison.iterrows():
            print(f"  Tamaño {row['size']}: {row['efficiency']:.2f}")
        
        # Generar gráfico
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        ax1.plot(comparison['size'], comparison['hit_rate'], marker='o', linewidth=2, markersize=8)
        ax1.set_xlabel('Tamaño de Caché')
        ax1.set_ylabel('Hit Rate (%)')
        ax1.set_title('Hit Rate vs Tamaño de Caché')
        ax1.grid(True)
        
        ax2.bar(comparison['size'], comparison['evictions'], color='#e74c3c')
        ax2.set_xlabel('Tamaño de Caché')
        ax2.set_ylabel('Número de Evictions')
        ax2.set_title('Evictions vs Tamaño de Caché')
        
        plt.tight_layout()
        plt.savefig('grafico_tamano.png', dpi=300, bbox_inches='tight')
        print("\n✓ Gráfico guardado: grafico_tamano.png")

def analyze_traffic(df):
    """Análisis de distribuciones de tráfico."""
    print("\n" + "="*70)
    print("EXPERIMENTO 3: DISTRIBUCIONES DE TRÁFICO")
    print("="*70)
    
    # Filtrar: política LRU, tamaño 1500, sin TTL
    traffic_data = df[(df['policy'] == 'LRU') & (df['size'] == 1500) & (df['ttl'] == 0)]
    
    if len(traffic_data) > 0:
        comparison = traffic_data[['sleep', 'hits', 'misses', 'hit_rate']].sort_values('sleep')
        comparison['frecuencia'] = comparison['sleep'].apply(lambda x: 'Alta (0.5s)' if x < 1 else ('Media (1.5s)' if x < 2 else 'Baja (3.0s)'))
        
        print("\nResultados:")
        print(comparison[['frecuencia', 'hits', 'misses', 'hit_rate']].to_string(index=False))
        
        print(f"\n✓ Análisis:")
        for _, row in comparison.iterrows():
            total_requests = row['hits'] + row['misses']
            print(f"  {row['frecuencia']}: {total_requests} consultas, {row['hit_rate']:.2f}% hit rate")

def analyze_ttl(df):
    """Análisis del impacto del TTL."""
    print("\n" + "="*70)
    print("EXPERIMENTO 4: IMPACTO DEL TTL (TIME TO LIVE)")
    print("="*70)
    
    # Filtrar: política LRU, tamaño 1500, tráfico normal
    ttl_data = df[(df['policy'] == 'LRU') & (df['size'] == 1500) & (df['sleep'] == 1.5) & (df['ttl'] > 0)]
    
    if len(ttl_data) > 0:
        comparison = ttl_data[['ttl', 'hits', 'misses', 'evictions', 'expirations', 'hit_rate']].sort_values('ttl')
        
        print("\nResultados:")
        print(comparison.to_string(index=False))
        
        # Comparar con sin TTL
        no_ttl = df[(df['policy'] == 'LRU') & (df['size'] == 1500) & (df['sleep'] == 1.5) & (df['ttl'] == 0)]
        if len(no_ttl) > 0:
            baseline = no_ttl.iloc[0]['hit_rate']
            print(f"\n✓ Hit rate sin TTL (baseline): {baseline:.2f}%")
            print(f"✓ Impacto de TTL en hit rate:")
            for _, row in comparison.iterrows():
                diff = row['hit_rate'] - baseline
                print(f"  TTL {row['ttl']}s: {row['hit_rate']:.2f}% ({diff:+.2f}% vs baseline)")
                print(f"    Expiraciones: {row['expirations']}")
        
        # Generar gráfico
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        ax1.plot(comparison['ttl'], comparison['hit_rate'], marker='o', linewidth=2, markersize=8, color='#3498db')
        if len(no_ttl) > 0:
            ax1.axhline(y=baseline, color='r', linestyle='--', label=f'Sin TTL ({baseline:.1f}%)')
            ax1.legend()
        ax1.set_xlabel('TTL (segundos)')
        ax1.set_ylabel('Hit Rate (%)')
        ax1.set_title('Impacto del TTL en Hit Rate')
        ax1.grid(True)
        
        ax2.bar(comparison['ttl'].astype(str), comparison['expirations'], color='#e67e22')
        ax2.set_xlabel('TTL (segundos)')
        ax2.set_ylabel('Número de Expiraciones')
        ax2.set_title('Expiraciones por TTL')
        
        plt.tight_layout()
        plt.savefig('grafico_ttl.png', dpi=300, bbox_inches='tight')
        print("\n✓ Gráfico guardado: grafico_ttl.png")

def generate_summary_table(df):
    """Genera tabla resumen de todos los experimentos."""
    print("\n" + "="*70)
    print("TABLA RESUMEN DE TODOS LOS EXPERIMENTOS")
    print("="*70)
    
    summary = df[['experiment', 'policy', 'size', 'ttl', 'hits', 'misses', 'evictions', 'expirations', 'hit_rate']].copy()
    summary = summary.sort_values('hit_rate', ascending=False)
    
    print("\n" + summary.to_string(index=False))
    
    # Guardar a CSV
    summary.to_csv('resumen_experimentos.csv', index=False)
    print("\n✓ Tabla guardada: resumen_experimentos.csv")

def main():
    print("="*70)
    print("ANÁLISIS DE EXPERIMENTOS DE CACHÉ")
    print("="*70)
    
    # Cargar datos
    df = load_experiments()
    
    if len(df) == 0:
        print("\n❌ No se encontraron resultados de experimentos.")
        print("   Ejecuta primero: ./run_experiments.sh")
        return
    
    print(f"\n✓ Cargados {len(df)} experimentos")
    
    # Análisis
    analyze_policies(df)
    analyze_cache_size(df)
    analyze_traffic(df)
    analyze_ttl(df)
    generate_summary_table(df)
    
    print("\n" + "="*70)
    print("✓ ANÁLISIS COMPLETADO")
    print("="*70)
    print("\nArchivos generados:")
    print("  - grafico_politicas.png")
    print("  - grafico_tamano.png")
    print("  - grafico_ttl.png")
    print("  - resumen_experimentos.csv")
    print("")

if __name__ == "__main__":
    main()
