using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using MySql.Data.MySqlClient;
using System.Text;
using System.Linq;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Data;


namespace GBasesDatos
{
    class Program
    {
        static string connectionString = "Server=localhost;Database=pruebadb;Uid=root;Pwd=123456789;";

        static void Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine("");
                Console.WriteLine("Seleccione una opción:");
                Console.WriteLine("1. Listar atributos por entidades");
                Console.WriteLine("2. Generar disparadores de auditoría");
                Console.WriteLine("3. Ejecutar consultas con hilos");
                Console.WriteLine("4. Ver historial de consultas");
                Console.WriteLine("5. Salir");

                Console.WriteLine("");
                Console.Write("Ingrese la opción seleccionada: ");

                int opcion;
                if (!int.TryParse(Console.ReadLine(), out opcion))
                {
                    Console.WriteLine("Opción no válida. Por favor, ingrese un número del 1 al 5.");
                    continue;
                }

                switch (opcion)
                {
                    case 1:
                        ListarAtributosEntidad();
                        break;
                    case 2:
                        string outputFilePath = "C:/Users/Kristher/Desktop/Bryan/2doParcial/triggersAuditoria.sql";
                        GenerateTriggers(outputFilePath);
                        break;
                    case 3:
                        EjecutarConsultasConHilos();
                        break;
                    case 4:
                        VerHistorialDeConsultas();
                        break;
                    case 5:
                        return;
                    default:
                        Console.WriteLine("Opción no válida. Por favor, ingrese un número del 1 al 5.");
                        break;
                }
            }
        }
        private static List<string> historialConsultas = new List<string>();

        static void ListarAtributosEntidad()
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    connection.Open();

                    string query = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'pruebadb' ORDER BY TABLE_NAME;";

                    Dictionary<int, string> entityDictionary = new Dictionary<int, string>();

                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            Console.WriteLine("Lista de entidades en la base de datos:");

                            int entityId = 1;
                            while (reader.Read())
                            {
                                string entityName = reader.GetString(1);
                                Console.WriteLine($"{entityId}. {entityName}");
                                entityDictionary.Add(entityId, entityName);
                                entityId++;
                            }
                        }
                    }

                    Console.Write("Seleccione el número de la entidad para ver sus atributos (o 0 para salir): ");
                    if (int.TryParse(Console.ReadLine(), out int selectedEntityId) && entityDictionary.ContainsKey(selectedEntityId))
                    {
                        string selectedEntity = entityDictionary[selectedEntityId];
                        MostrarAtributosEntidad(connection, selectedEntity);
                    }
                    else
                    {
                        Console.WriteLine("Selección no válida o no seleccionada. Saliendo del programa.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error al listar los atributos de las entidades: " + ex.Message);
            }
        }

        static void MostrarAtributosEntidad(MySqlConnection connection, string entityName)
        {
            try
            {
                string query = "SELECT column_name FROM information_schema.columns WHERE table_name = @entityName";

                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@entityName", entityName);

                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Atributos de la entidad " + entityName + ":");

                        while (reader.Read())
                        {
                            string attributeName = reader.GetString(0);
                            Console.WriteLine("- " + attributeName);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error al listar los atributos de la entidad " + entityName + ": " + ex.Message);
            }
        }

        public static void GenerateTriggers(string outputFilePath)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();

                string query = @"
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'pruebadb'
                ORDER BY table_name, column_name";

                MySqlCommand command = new MySqlCommand(query, connection);
                MySqlDataReader reader = command.ExecuteReader();

                StringBuilder script = new StringBuilder();

                string currentTable = null;
                StringBuilder newValues = new StringBuilder();
                StringBuilder oldValues = new StringBuilder();
                string primaryKey = null;

                while (reader.Read())
                {
                    string tableName = reader["table_name"].ToString();
                    string columnName = reader["column_name"].ToString();

                    if (currentTable != tableName)
                    {
                        if (currentTable != null)
                        {
                            AppendTriggerScripts(script, currentTable, oldValues.ToString(), newValues.ToString(), primaryKey);
                        }

                        currentTable = tableName;
                        newValues.Clear();
                        oldValues.Clear();
                        primaryKey = columnName; // Assuming the first column is the primary key
                    }

                    newValues.Append($"'New_{columnName}: ' CONVERT(NVARCHAR(MAX), NEW.{columnName}) + ', ' ");
                    oldValues.Append($"'Old_{columnName}: ' CONVERT(NVARCHAR(MAX), OLD.{columnName}) + ', ' ");
                }

                if (currentTable != null)
                {
                    AppendTriggerScripts(script, currentTable, oldValues.ToString(), newValues.ToString(), primaryKey);
                }

                reader.Close();

                File.WriteAllText(outputFilePath, script.ToString());
                Console.WriteLine($"El archivo con los triggers ha sido generado en: {outputFilePath}");

            }
        }

        private static void AppendTriggerScripts(StringBuilder script, string tableName, string oldValues, string newValues, string primaryKey)
        {
            script.AppendLine($"-- Triggers for {tableName}");
            script.AppendLine(GenerateInsertTrigger(tableName, newValues, primaryKey));
            script.AppendLine(GenerateUpdateTrigger(tableName, oldValues, newValues, primaryKey));
            script.AppendLine(GenerateDeleteTrigger(tableName, oldValues, primaryKey));
            script.AppendLine();
        }

        private static string GenerateInsertTrigger(string tableName, string newValues, string primaryKey)
        {
            return $@"
DELIMITER //
CREATE TRIGGER trg_{tableName}_Insert
AFTER INSERT ON {tableName}
FOR EACH ROW
BEGIN
    INSERT INTO Auditoria (TableName, Action, RecordId, NewValues, Timestamp, User)
    VALUES ('{tableName}', 'INSERT', NEW.{primaryKey},
           CONCAT('New_', '{primaryKey}: ', NEW.{primaryKey}, ', ',
                  '{newValues.Replace("'", "''").Replace(", ", "', 'New_")}'),
           NOW(), USER());
END//
DELIMITER ;
";
        }

        private static string GenerateUpdateTrigger(string tableName, string oldValues, string newValues, string primaryKey)
        {
            return $@"
DELIMITER //
CREATE TRIGGER trg_{tableName}_Update
AFTER UPDATE ON {tableName}
FOR EACH ROW
BEGIN
    INSERT INTO Auditoria (TableName, Action, RecordId, OldValues, NewValues, Timestamp, User)
    VALUES ('{tableName}', 'UPDATE', OLD.{primaryKey},
           CONCAT('Old_', '{primaryKey}: ', OLD.{primaryKey}, ', ',
                  '{oldValues.Replace("'", "''").Replace(", ", "', 'Old_")}'),
           CONCAT('New_', '{primaryKey}: ', NEW.{primaryKey}, ', ',
                  '{newValues.Replace("'", "''").Replace(", ", "', 'New_")}'),
           NOW(), USER());
END//
DELIMITER ;
";
        }

        private static string GenerateDeleteTrigger(string tableName, string oldValues, string primaryKey)
        {
            return $@"
DELIMITER //
CREATE TRIGGER trg_{tableName}_Delete
AFTER DELETE ON {tableName}
FOR EACH ROW
BEGIN
    INSERT INTO Auditoria (TableName, Action, RecordId, OldValues, Timestamp, User)
    VALUES ('{tableName}', 'DELETE', OLD.{primaryKey},
           CONCAT('Old_', '{primaryKey}: ', OLD.{primaryKey}, ', ',
                  '{oldValues.Replace("'", "''").Replace(", ", "', 'Old_")}'),
           NOW(), USER());
END//
DELIMITER ;
";
        }


        static void EjecutarConsultasConHilos()
        {
            string filePath = @"C:\Users\mat-1\Desktop\Queryt\Querys.txt";
            List<string> queries = new List<string>();

            Console.WriteLine("Seleccione una opción:");
            Console.WriteLine("1. Ingresar consultas manualmente");
            Console.WriteLine("2. Leer consultas desde archivo");
            Console.Write("Ingrese la opción seleccionada: ");

            if (int.TryParse(Console.ReadLine(), out int opcion) && opcion == 2)
            {
                // Leer las consultas desde el archivo.txt
                try
                {
                    if (File.Exists(filePath))
                    {
                        queries.AddRange(File.ReadAllLines(filePath));
                    }
                    else
                    {
                        Console.WriteLine("El archivo de consultas no existe en la ruta especificada.");
                        return;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al leer el archivo de consultas: {ex.Message}");
                    return;
                }

                if (queries.Count == 0)
                {
                    Console.WriteLine("No se encontraron consultas en el archivo.");
                    return;
                }
            }
            else if (opcion == 1)
            {
                // Solicitar al usuario el número de consultas
                Console.Write("Ingrese el número de consultas a ejecutar: ");
                if (!int.TryParse(Console.ReadLine(), out int numConsultas) || numConsultas <= 0)
                {
                    Console.WriteLine("Número de consultas no válido.");
                    return;
                }

                // Solicitar al usuario las consultas
                for (int i = 0; i < numConsultas; i++)
                {
                    Console.Write($"Ingrese la consulta {i + 1}: ");
                    string query = Console.ReadLine();
                    queries.Add(query);
                }
            }
            else
            {
                Console.WriteLine("Opción no válida.");
                return;
            }

            // Añadir las consultas al historial
            historialConsultas.AddRange(queries);

            // Lista para almacenar los resultados de cada consulta
            List<DataTable> results = new List<DataTable>();
            List<long> tiempos = new List<long>();

            // Crear y ejecutar cada consulta en un hilo separado
            List<Task> tasks = new List<Task>();
            foreach (string query in queries)
            {
                Task task = Task.Run(() =>
                {
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    DataTable result = EjecutarConsulta(query);
                    stopwatch.Stop();

                    lock (results) // Bloquea el acceso a las listas compartidas
                    {
                        results.Add(result);
                        tiempos.Add(stopwatch.ElapsedMilliseconds);
                    }
                });
                tasks.Add(task);
            }

            // Esperar a que todos los hilos terminen
            Task.WaitAll(tasks.ToArray());

            // Mostrar los resultados
            Console.WriteLine("Resultados de las consultas:");
            for (int i = 0; i < results.Count; i++)
            {
                Console.WriteLine($"Resultado de la consulta {i + 1} (Tiempo: {tiempos[i]} ms):");
                MostrarResultadoConsulta(results[i]);
            }
        }

        static DataTable EjecutarConsulta(string query)
        {
            DataTable result = new DataTable();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataAdapter adapter = new MySqlDataAdapter(command))
                        {
                            adapter.Fill(result);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al ejecutar la consulta: {ex.Message}");
            }

            return result;
        }

        static void MostrarResultadoConsulta(DataTable result)
        {
            foreach (DataRow row in result.Rows)
            {
                foreach (DataColumn column in result.Columns)
                {
                    Console.Write($"{column.ColumnName}: {row[column]} ");
                }
                Console.WriteLine();
            }
        }

        static void VerHistorialDeConsultas()
        {
            Console.WriteLine("Historial de consultas realizadas:");
            if (historialConsultas.Count > 0)
            {
                for (int i = 0; i < historialConsultas.Count; i++)
                {
                    Console.WriteLine($"{i + 1}. {historialConsultas[i]}");
                }
            }
            else
            {
                Console.WriteLine("No hay consultas realizadas aun");
            }
        }
    }
}