import happybase
import csv
import sys
from datetime import datetime

def sanitize(value):
    """Limpia caracteres problemáticos para HBase"""
    return str(value).replace('\x00', '').replace('\n', ' ').replace('\t', ' ')

def connect_hbase():
    """Establece conexión con HBase"""
    try:
        connection = happybase.Connection('localhost', timeout=10000)
        connection.open()
        return connection
    except Exception as e:
        print(f"\n❌ Error conectando a HBase: {str(e)}")
        print("Verifica que:")
        print("1. El servidor Thrift está corriendo (ejecuta: hbase-daemon.sh start thrift)")
        print("2. HappyBase está instalado (ejecuta: pip install happybase)")
        sys.exit(1)

def print_progress(current, total, start_time):
    """Muestra el progreso de la carga"""
    elapsed = datetime.now() - start_time
    percent = (current / total) * 100
    speed = current / max(1, elapsed.total_seconds())
    print(f"\r⏳ Progreso: {current}/{total} ({percent:.1f}%) | "
          f"Velocidad: {speed:.1f} reg/s | "
          f"Tiempo: {str(elapsed).split('.')[0]}", end='', flush=True)

def load_data(csv_file, batch_size=1000):
    """Carga los datos desde CSV a HBase"""
    print("🚀 Iniciando carga de datos en HBase...")
    
    # Conexión a HBase
    connection = connect_hbase()
    table = connection.table('accidents_data')
    
    try:
        # Contar filas totales
        with open(csv_file, 'r') as f:
            total_rows = sum(1 for _ in f) - 1
        
        print(f"📊 Total de registros a procesar: {total_rows}")
        print("--------------------------------------------------")
        
        start_time = datetime.now()
        processed_rows = 0
        batch = table.batch(batch_size=batch_size)
        
        with open(csv_file, 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            
            for row in csvreader:
                try:
                    accident_id = sanitize(row['Accident ID'])
                    
                    # Preparar datos para HBase
                    data = {
                        b'accident_info:date': sanitize(row['Date']).encode('utf-8'),
                        b'accident_info:time': sanitize(row['Time']).encode('utf-8'),
                        b'location:place': sanitize(row['Location']).encode('utf-8'),
                        b'location:latitude': sanitize(row['Latitude']).encode('utf-8'),
                        b'location:longitude': sanitize(row['Longitude']).encode('utf-8'),
                        b'conditions:weather': sanitize(row['Weather Condition']).encode('utf-8'),
                        b'conditions:road': sanitize(row['Road Condition']).encode('utf-8'),
                        b'stats:vehicles': sanitize(row['Vehicles Involved']).encode('utf-8'),
                        b'stats:casualties': sanitize(row['Casualties']).encode('utf-8'),
                        b'cause:reason': sanitize(row['Cause']).encode('utf-8')
                    }
                    
                    # Añadir a lote
                    batch.put(accident_id.encode('utf-8'), data)
                    processed_rows += 1
                    
                    # Mostrar progreso cada 100 registros
                    if processed_rows % 100 == 0:
                        print_progress(processed_rows, total_rows, start_time)
                        
                except Exception as e:
                    print(f"\n⚠️ Error procesando fila {processed_rows}: {str(e)}")
                    continue
        
        # Enviar último lote
        batch.send()
        print_progress(processed_rows, total_rows, start_time)
        
        # Resumen final
        elapsed_time = datetime.now() - start_time
        print("\n\n✅ Carga completada!")
        print(f"Total procesados: {processed_rows} de {total_rows} registros")
        print(f"Tiempo total: {str(elapsed_time).split('.')[0]}")
        print(f"Velocidad promedio: {processed_rows/max(1, elapsed_time.total_seconds()):.1f} registros/segundo")
        
    except Exception as e:
        print(f"\n🔥 Error durante la carga: {str(e)}")
    finally:
        connection.close()
        print("🔌 Conexión con HBase cerrada.")

if __name__ == "__main__":
    # Configuración
    csv_file = 'global_traffic_accidents.csv'
    batch_size = 500  # Ajusta según necesidades
    
    # Ejecutar carga
    load_data(csv_file, batch_size)
