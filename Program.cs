using System.Text;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Isql;

namespace DbMetaTool
{
    public static class Program
    {
        // Przykładowe wywołania:
        // DbMetaTool build-db --db-dir "C:\db\fb5" --scripts-dir "C:\scripts"
        // DbMetaTool export-scripts --connection-string "..." --output-dir "C:\out"
        // DbMetaTool update-db --connection-string "..." --scripts-dir "C:\scripts"
        public static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Użycie:");
                Console.WriteLine("  build-db --db-dir <ścieżka> --scripts-dir <ścieżka>");
                Console.WriteLine("  export-scripts --connection-string <connStr> --output-dir <ścieżka>");
                Console.WriteLine("  update-db --connection-string <connStr> --scripts-dir <ścieżka>");
                return 1;
            }

            try
            {
                var command = args[0].ToLowerInvariant();

                switch (command)
                {
                    case "build-db":
                        {
                            string dbDir = GetArgValue(args, "--db-dir");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            BuildDatabase(dbDir, scriptsDir);
                            Console.WriteLine("Baza danych została zbudowana pomyślnie.");
                            return 0;
                        }

                    case "export-scripts":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string outputDir = GetArgValue(args, "--output-dir");

                            ExportScripts(connStr, outputDir);
                            Console.WriteLine("Skrypty zostały wyeksportowane pomyślnie.");
                            return 0;
                        }

                    case "update-db":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            UpdateDatabase(connStr, scriptsDir);
                            Console.WriteLine("Baza danych została zaktualizowana pomyślnie.");
                            return 0;
                        }

                    default:
                        Console.WriteLine($"Nieznane polecenie: {command}");
                        return 1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd: " + ex.Message);
                return -1;
            }
        }

        private static string GetArgValue(string[] args, string name)
        {
            int idx = Array.IndexOf(args, name);
            if (idx == -1 || idx + 1 >= args.Length)
                throw new ArgumentException($"Brak wymaganego parametru {name}");
            return args[idx + 1];
        }

        /// <summary>
        /// Buduje nową bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void BuildDatabase(string databaseDirectory, string scriptsDirectory)
        {
            // 1. Create database directory
            try
            {
                Directory.CreateDirectory(databaseDirectory);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas tworzenia katalogu bazy danych: {ex.Message}");
                throw;
            }

            // 2. Create empty Firebird 5.0 database
            var dbFilePath = Path.Combine(databaseDirectory, "database.fdb");
            var connectionString = $"User=SYSDBA;Password=masterkey;Database={dbFilePath};DataSource=localhost;Port=3050;";

            try
            {
                if (File.Exists(dbFilePath))
                {
                    Console.WriteLine("Ostrzeżenie: Plik bazy danych już istnieje. Zostanie nadpisany.");
                    File.Delete(dbFilePath);
                }
                FbConnection.CreateDatabase(connectionString, overwrite: true);
                Console.WriteLine($"Utworzono pustą bazę danych: {dbFilePath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas tworzenia bazy danych: {ex.Message}");
                throw;
            }

            // 3. Connect to the new database
            using var connection = new FbConnection(connectionString);
            connection.Open();
            Console.WriteLine("Połączono z nową bazą danych.");

            // 4. Execute scripts in order: domains → tables → procedures
            int domainsExecuted = 0, tablesExecuted = 0, proceduresExecuted = 0;
            int domainsErrors = 0, tablesErrors = 0, proceduresErrors = 0;

            // Execute domain scripts
            var domainsDir = Path.Combine(scriptsDirectory, "domains");
            if (Directory.Exists(domainsDir))
            {
                try
                {
                    var domainFiles = Directory.GetFiles(domainsDir, "*.sql");
                    foreach (var scriptFile in domainFiles)
                    {
                        try
                        {
                            var scriptContent = File.ReadAllText(scriptFile);
                            ExecuteScript(connection, scriptContent);
                            domainsExecuted++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Błąd podczas wykonywania skryptu domeny {Path.GetFileName(scriptFile)}: {ex.Message}");
                            domainsErrors++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Błąd podczas przetwarzania katalogu domen: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine($"Ostrzeżenie: Katalog domen nie istnieje: {domainsDir}");
            }

            // Execute table scripts
            var tablesDir = Path.Combine(scriptsDirectory, "tables");
            if (Directory.Exists(tablesDir))
            {
                try
                {
                    var tableFiles = Directory.GetFiles(tablesDir, "*.sql");
                    foreach (var scriptFile in tableFiles)
                    {
                        try
                        {
                            var scriptContent = File.ReadAllText(scriptFile);
                            ExecuteScript(connection, scriptContent);
                            tablesExecuted++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Błąd podczas wykonywania skryptu tabeli {Path.GetFileName(scriptFile)}: {ex.Message}");
                            tablesErrors++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Błąd podczas przetwarzania katalogu tabel: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine($"Ostrzeżenie: Katalog tabel nie istnieje: {tablesDir}");
            }

            // Execute procedure scripts
            var proceduresDir = Path.Combine(scriptsDirectory, "procedures");
            if (Directory.Exists(proceduresDir))
            {
                try
                {
                    var procedureFiles = Directory.GetFiles(proceduresDir, "*.sql");
                    foreach (var scriptFile in procedureFiles)
                    {
                        try
                        {
                            var scriptContent = File.ReadAllText(scriptFile);
                            ExecuteScriptWithTerminator(connection, scriptContent);
                            proceduresExecuted++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Błąd podczas wykonywania skryptu procedury {Path.GetFileName(scriptFile)}: {ex.Message}");
                            proceduresErrors++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Błąd podczas przetwarzania katalogu procedur: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine($"Ostrzeżenie: Katalog procedur nie istnieje: {proceduresDir}");
            }

            // Display final report
            Console.WriteLine();
            Console.WriteLine($"Domeny: {domainsExecuted} wykonanych, {domainsErrors} błędów");
            Console.WriteLine($"Tabele: {tablesExecuted} wykonanych, {tablesErrors} błędów");
            Console.WriteLine($"Procedury: {proceduresExecuted} wykonanych, {proceduresErrors} błędów");
            Console.WriteLine($"Łącznie: {domainsExecuted + tablesExecuted + proceduresExecuted} wykonanych, {domainsErrors + tablesErrors + proceduresErrors} błędów");

            if (domainsErrors + tablesErrors + proceduresErrors > 0)
            {
                throw new Exception("Budowanie bazy danych zakończone z błędami.");
            }
        }

        private static void ExecuteScriptWithTerminator(FbConnection connection, string scriptContent)
        {
            if (string.IsNullOrWhiteSpace(scriptContent))
                return;

            // Use FbScript to handle SET TERM statements properly
            var script = new FbScript(scriptContent);
            script.Parse();

            using var transaction = connection.BeginTransaction();
            try
            {
                foreach (var statement in script.Results)
                {
                    var cmd = new FbCommand(statement.Text, connection, transaction);
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public static void ExportScripts(string connectionString, string outputDirectory)
        {
            try
            {
                Directory.CreateDirectory(outputDirectory);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas tworzenia katalogu wyjściowego: {ex.Message}");
                throw;
            }

            using var connection = new FbConnection(connectionString);
            connection.Open();

            Console.WriteLine("Połączono z bazą danych.");

            // Export domains
            var domainsDir = Path.Combine(outputDirectory, "domains");
            try
            {
                Directory.CreateDirectory(domainsDir);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas tworzenia katalogu domen: {ex.Message}");
                throw;
            }

            try
            {
                ExportDomains(connection, domainsDir);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas eksportu domen: {ex.Message}");
            }

            // Export tables
            var tablesDir = Path.Combine(outputDirectory, "tables");
            try
            {
                Directory.CreateDirectory(tablesDir);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas tworzenia katalogu tabel: {ex.Message}");
                throw;
            }

            try
            {
                ExportTables(connection, tablesDir);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas eksportu tabel: {ex.Message}");
            }

            // Export procedures
            var proceduresDir = Path.Combine(outputDirectory, "procedures");
            try
            {
                Directory.CreateDirectory(proceduresDir);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas tworzenia katalogu procedur: {ex.Message}");
                throw;
            }

            try
            {
                ExportProcedures(connection, proceduresDir);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas eksportu procedur: {ex.Message}");
            }
        }

        /// <summary>
        /// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void UpdateDatabase(string connectionString, string scriptsDirectory)
        {
            // 1. Connect to the existing database
            using var connection = new FbConnection(connectionString);
            try
            {
                connection.Open();
                Console.WriteLine("Połączono z bazą danych.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd podczas łączenia z bazą danych: {ex.Message}");
                throw;
            }

            // 2. Execute scripts in order: domains → tables → procedures
            int domainsExecuted = 0, tablesExecuted = 0, proceduresExecuted = 0;
            int domainsSkipped = 0, tablesSkipped = 0, proceduresSkipped = 0;
            int domainsErrors = 0, tablesErrors = 0, proceduresErrors = 0;

            // Execute domain scripts
            var domainsDir = Path.Combine(scriptsDirectory, "domains");
            if (Directory.Exists(domainsDir))
            {
                try
                {
                    var domainFiles = Directory.GetFiles(domainsDir, "*.sql");
                    foreach (var scriptFile in domainFiles)
                    {
                        try
                        {
                            var scriptContent = File.ReadAllText(scriptFile);
                            ExecuteScript(connection, scriptContent);
                            domainsExecuted++;
                            Console.WriteLine($"Wykonano: {Path.GetFileName(scriptFile)}");
                        }
                        catch (FbException fbEx) when (fbEx.Message.Contains("already exists") || fbEx.ErrorCode == 335544351)
                        {
                            // Domain already exists - skip it
                            domainsSkipped++;
                            Console.WriteLine($"Pominięto (już istnieje): {Path.GetFileName(scriptFile)}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Błąd podczas wykonywania skryptu domeny {Path.GetFileName(scriptFile)}: {ex.Message}");
                            domainsErrors++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Błąd podczas przetwarzania katalogu domen: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine($"Ostrzeżenie: Katalog domen nie istnieje: {domainsDir}");
            }

            // Execute table scripts
            var tablesDir = Path.Combine(scriptsDirectory, "tables");
            if (Directory.Exists(tablesDir))
            {
                try
                {
                    var tableFiles = Directory.GetFiles(tablesDir, "*.sql");
                    foreach (var scriptFile in tableFiles)
                    {
                        try
                        {
                            var scriptContent = File.ReadAllText(scriptFile);
                            ExecuteScript(connection, scriptContent);
                            tablesExecuted++;
                            Console.WriteLine($"Wykonano: {Path.GetFileName(scriptFile)}");
                        }
                        catch (FbException fbEx) when (fbEx.Message.Contains("already exists") || fbEx.ErrorCode == 335544351)
                        {
                            // Table already exists - skip it
                            tablesSkipped++;
                            Console.WriteLine($"Pominięto (już istnieje): {Path.GetFileName(scriptFile)}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Błąd podczas wykonywania skryptu tabeli {Path.GetFileName(scriptFile)}: {ex.Message}");
                            tablesErrors++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Błąd podczas przetwarzania katalogu tabel: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine($"Ostrzeżenie: Katalog tabel nie istnieje: {tablesDir}");
            }

            // Execute procedure scripts
            var proceduresDir = Path.Combine(scriptsDirectory, "procedures");
            if (Directory.Exists(proceduresDir))
            {
                try
                {
                    var procedureFiles = Directory.GetFiles(proceduresDir, "*.sql");
                    foreach (var scriptFile in procedureFiles)
                    {
                        try
                        {
                            var scriptContent = File.ReadAllText(scriptFile);
                            ExecuteScriptWithTerminator(connection, scriptContent);
                            proceduresExecuted++;
                            Console.WriteLine($"Wykonano: {Path.GetFileName(scriptFile)}");
                        }
                        catch (FbException fbEx) when (fbEx.Message.Contains("already exists") || fbEx.ErrorCode == 335544351)
                        {
                            // Procedure already exists - skip it
                            proceduresSkipped++;
                            Console.WriteLine($"Pominięto (już istnieje): {Path.GetFileName(scriptFile)}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Błąd podczas wykonywania skryptu procedury {Path.GetFileName(scriptFile)}: {ex.Message}");
                            proceduresErrors++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Błąd podczas przetwarzania katalogu procedur: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine($"Ostrzeżenie: Katalog procedur nie istnieje: {proceduresDir}");
            }

            // Display final report
            Console.WriteLine();
            Console.WriteLine($"Domeny: {domainsExecuted} wykonanych, {domainsSkipped} pominiętych, {domainsErrors} błędów");
            Console.WriteLine($"Tabele: {tablesExecuted} wykonanych, {tablesSkipped} pominiętych, {tablesErrors} błędów");
            Console.WriteLine($"Procedury: {proceduresExecuted} wykonanych, {proceduresSkipped} pominiętych, {proceduresErrors} błędów");
            Console.WriteLine($"Łącznie: {domainsExecuted + tablesExecuted + proceduresExecuted} wykonanych, {domainsSkipped + tablesSkipped + proceduresSkipped} pominiętych, {domainsErrors + tablesErrors + proceduresErrors} błędów");

            if (domainsErrors + tablesErrors + proceduresErrors > 0)
            {
                throw new Exception("Aktualizacja bazy danych zakończona z błędami.");
            }
        }

        private static void ExecuteScript(FbConnection connection, string scriptContent)
        {
            if (string.IsNullOrWhiteSpace(scriptContent))
                return;

            using var transaction = connection.BeginTransaction();
            try
            {
                using var cmd = new FbCommand(scriptContent, connection, transaction);
                cmd.ExecuteNonQuery();
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        private static void ExportDomains(FbConnection connection, string outputDirectory)
        {
            const string query = @"
                SELECT
                    TRIM(RDB$FIELD_NAME) AS DOMAIN_NAME,
                    RDB$FIELD_TYPE,
                    RDB$FIELD_LENGTH,
                    RDB$FIELD_SCALE,
                    RDB$CHARACTER_LENGTH,
                    RDB$FIELD_SUB_TYPE,
                    TRIM(RDB$DEFAULT_SOURCE) AS DEFAULT_VALUE,
                    RDB$NULL_FLAG,
                    TRIM(RDB$VALIDATION_SOURCE) AS CHECK_CONSTRAINT
                FROM RDB$FIELDS
                WHERE (RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL) AND RDB$FIELD_NAME NOT STARTING WITH 'RDB$'
                ORDER BY RDB$FIELD_NAME";

            using var cmd = new FbCommand(query, connection);
            using var reader = cmd.ExecuteReader();

            int count = 0;
            while (reader.Read())
            {
                var domainName = reader.GetString(0);
                var sqlType = GetSqlType(
                    fieldType: reader.GetInt16(1),
                    fieldLength: reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                    fieldScale: reader.IsDBNull(3) ? (short)0 : reader.GetInt16(3),
                    charLength: reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4),
                    subType: reader.IsDBNull(5) ? (short?)null : reader.GetInt16(5)
                );

                var sb = new StringBuilder();
                sb.AppendLine($"CREATE DOMAIN {domainName} AS {sqlType}");

                if (!reader.IsDBNull(6))
                {
                    sb.AppendLine($"  {reader.GetString(6)}");
                }

                if (!reader.IsDBNull(7) && reader.GetInt16(7) == 1)
                {
                    sb.AppendLine("  NOT NULL");
                }

                if (!reader.IsDBNull(8))
                {
                    sb.AppendLine($"  {reader.GetString(8)}");
                }

                sb.AppendLine(";");

                var fileName = Path.Combine(outputDirectory, $"{domainName}.sql");
                File.WriteAllText(fileName, sb.ToString());
                count++;
            }

            Console.WriteLine($"Wyeksportowano {count} domen.");
        }

        private static void ExportTables(FbConnection connection, string outputDirectory)
        {
            // Get all user tables
            const string tablesQuery = @"
                SELECT TRIM(RDB$RELATION_NAME) AS TABLE_NAME
                FROM RDB$RELATIONS
                WHERE RDB$VIEW_BLR IS NULL
                  AND (RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL)
                ORDER BY RDB$RELATION_NAME";

            using var cmd = new FbCommand(tablesQuery, connection);
            using var reader = cmd.ExecuteReader();

            var tableNames = new List<string>();
            while (reader.Read())
            {
                tableNames.Add(reader.GetString(0));
            }

            foreach (var tableName in tableNames)
            {
                var sb = new StringBuilder();
                sb.AppendLine($"CREATE TABLE {tableName} (");

                // Get columns for this table
                const string columnsQuery = @"
                    SELECT
                        TRIM(rf.RDB$FIELD_NAME) AS COLUMN_NAME,
                        rf.RDB$FIELD_POSITION AS FIELD_POSITION,
                        f.RDB$FIELD_TYPE,
                        f.RDB$FIELD_LENGTH,
                        f.RDB$FIELD_SCALE,
                        f.RDB$CHARACTER_LENGTH,
                        f.RDB$FIELD_SUB_TYPE,
                        rf.RDB$NULL_FLAG,
                        TRIM(rf.RDB$DEFAULT_SOURCE) AS DEFAULT_VALUE
                    FROM RDB$RELATION_FIELDS rf
                    JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
                    WHERE rf.RDB$RELATION_NAME = @TableName
                    ORDER BY rf.RDB$FIELD_POSITION";

                using var colCmd = new FbCommand(columnsQuery, connection);
                colCmd.Parameters.AddWithValue("@TableName", tableName);
                using var colReader = colCmd.ExecuteReader();

                var columnDefs = new List<string>();
                while (colReader.Read())
                {
                    var columnName = colReader.GetString(0);
                    var sqlType = GetSqlType(
                        colReader.GetInt16(2),
                        colReader.IsDBNull(3) ? 0 : colReader.GetInt32(3),
                        colReader.IsDBNull(4) ? (short)0 : colReader.GetInt16(4),
                        colReader.IsDBNull(5) ? (int?)null : colReader.GetInt32(5),
                        colReader.IsDBNull(6) ? (short?)null : colReader.GetInt16(6)
                    );

                    var colDef = $"  {columnName} {sqlType}";

                    if (!colReader.IsDBNull(8))
                    {
                        colDef += $" {colReader.GetString(8)}";
                    }

                    if (!colReader.IsDBNull(7) && colReader.GetInt16(7) == 1)
                    {
                        colDef += " NOT NULL";
                    }

                    columnDefs.Add(colDef);
                }

                sb.AppendLine(string.Join(",\n", columnDefs));
                sb.AppendLine(");");

                var fileName = Path.Combine(outputDirectory, $"{tableName}.sql");
                File.WriteAllText(fileName, sb.ToString());
            }

            Console.WriteLine($"Wyeksportowano {tableNames.Count} tabel.");
        }

        private static void ExportProcedures(FbConnection connection, string outputDirectory)
        {
            const string query = @"
                SELECT
                    TRIM(RDB$PROCEDURE_NAME) AS PROCEDURE_NAME,
                    RDB$PROCEDURE_SOURCE AS SOURCE_CODE
                FROM RDB$PROCEDURES
                WHERE (RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL)
                ORDER BY RDB$PROCEDURE_NAME";

            using var cmd = new FbCommand(query, connection);
            using var reader = cmd.ExecuteReader();

            int count = 0;
            while (reader.Read())
            {
                var procedureName = reader.GetString(0);
                var sourceCode = reader.IsDBNull(1) ? null : reader.GetString(1);

                var sb = new StringBuilder();
                sb.AppendLine("SET TERM ^ ;");
                sb.AppendLine();

                if (!string.IsNullOrWhiteSpace(sourceCode))
                {
                    sb.Append($"CREATE PROCEDURE {procedureName}");

                    // Get input parameters
                    var inputParams = GetProcedureParameters(connection, procedureName, 0);
                    if (inputParams.Count > 0)
                    {
                        sb.AppendLine(" (");
                        sb.AppendLine(string.Join(",\n", inputParams));
                        sb.Append(")");
                    }

                    // Get output parameters
                    var outputParams = GetProcedureParameters(connection, procedureName, 1);
                    if (outputParams.Count > 0)
                    {
                        sb.AppendLine();
                        sb.AppendLine("RETURNS (");
                        sb.AppendLine(string.Join(",\n", outputParams));
                        sb.Append(")");
                    }

                    sb.AppendLine();
                    sb.AppendLine("AS");
                    sb.AppendLine(sourceCode);
                    sb.AppendLine("^");
                }
                else
                {
                    sb.AppendLine($"-- Procedure {procedureName} has no source code");
                }

                sb.AppendLine();
                sb.AppendLine("SET TERM ; ^");

                var fileName = Path.Combine(outputDirectory, $"{procedureName}.sql");
                File.WriteAllText(fileName, sb.ToString());
                count++;
            }

            Console.WriteLine($"Wyeksportowano {count} procedur.");
        }

        private static List<string> GetProcedureParameters(FbConnection connection, string procedureName, short parameterType)
        {
            const string query = @"
                SELECT
                    TRIM(pp.RDB$PARAMETER_NAME) AS PARAM_NAME,
                    pp.RDB$PARAMETER_NUMBER,
                    f.RDB$FIELD_TYPE,
                    f.RDB$FIELD_LENGTH,
                    f.RDB$FIELD_SCALE,
                    f.RDB$CHARACTER_LENGTH,
                    f.RDB$FIELD_SUB_TYPE
                FROM RDB$PROCEDURE_PARAMETERS pp
                JOIN RDB$FIELDS f ON pp.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
                WHERE pp.RDB$PROCEDURE_NAME = @ProcedureName
                  AND pp.RDB$PARAMETER_TYPE = @ParameterType
                ORDER BY pp.RDB$PARAMETER_NUMBER";

            using var cmd = new FbCommand(query, connection);
            cmd.Parameters.AddWithValue("@ProcedureName", procedureName);
            cmd.Parameters.AddWithValue("@ParameterType", parameterType);
            using var reader = cmd.ExecuteReader();

            var parameters = new System.Collections.Generic.List<string>();
            while (reader.Read())
            {
                var paramName = reader.GetString(0);
                var sqlType = GetSqlType(
                    reader.GetInt16(2),
                    reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    reader.IsDBNull(4) ? (short)0 : reader.GetInt16(4),
                    reader.IsDBNull(5) ? (int?)null : reader.GetInt32(5),
                    reader.IsDBNull(6) ? (short?)null : reader.GetInt16(6)
                );

                parameters.Add($"  {paramName} {sqlType}");
            }

            return parameters;
        }

        private static string GetSqlType(short fieldType, int fieldLength, short fieldScale, int? charLength, short? subType)
        {
            return fieldType switch
            {
                7 => fieldScale < 0 ? $"NUMERIC(4,{-fieldScale})" : "SMALLINT",
                8 => fieldScale < 0 ? $"NUMERIC(9,{-fieldScale})" : "INTEGER",
                10 => "FLOAT",
                12 => "DATE",
                13 => "TIME",
                14 => charLength.HasValue ? $"CHAR({charLength.Value})" : $"CHAR({fieldLength})",
                16 => fieldScale < 0 ? $"NUMERIC(18,{-fieldScale})" : "BIGINT",
                27 => "DOUBLE PRECISION",
                35 => "TIMESTAMP",
                37 => charLength.HasValue ? $"VARCHAR({charLength.Value})" : $"VARCHAR({fieldLength})",
                261 => subType == 1 ? "BLOB SUB_TYPE TEXT" : "BLOB",
                _ => $"UNKNOWN_TYPE_{fieldType}"
            };
        }
    }
}