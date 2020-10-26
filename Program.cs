using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using MySql.Data.MySqlClient;

namespace Library
{
    class Program
    {
        const string Host = "localhost";
        const string Database = "library";
        const string User = "root";
        const string Password = "";

        static readonly string[] Tables = new[] { "user", "book", "book_order" };

        static MySqlConnection connection;
        static void CreateConnection()
        {
            connection = new MySqlConnection($"Database={Database};Datasource={Host};User={User};Password={Password}");
        }

        static void PrintTable(string table)
        {
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT * FROM {table}";
            var reader = command.ExecuteReader();

            Console.WriteLine($"----------- Table {table} -----------");

            try
            {
                while (reader.Read())
                {
                    object[] values = new object[reader.FieldCount];
                    reader.GetValues(values);

                    string str = values.Aggregate((v1, v2) => $"{v1} | {v2}").ToString();
                    Console.WriteLine(str);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to print data from table {table}\nError details: {ex.Message}");
                return;
            }
            finally
            {
                reader?.Close();
            }

            Console.WriteLine();
        }

        static void FillData(string table, string[] lines)
        {
            foreach (string line in lines)
            {
                var command = connection.CreateCommand();
                command.CommandText = $"INSERT INTO {table} VALUES ({line.Replace(";", ", ")})";
                command.ExecuteNonQuery();
            }
        }

        static void FillTables()
        {
            foreach (string table in Tables)
            {
                try
                {
                    string[] lines = File.ReadAllLines($"{table}.csv");
                    FillData(table, lines);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to fill data in table {table}\nError details: {ex.Message}");
                }
            }
        }

        static void ClearTables()
        {
            foreach (string table in Tables.Reverse())
            {
                var command = connection.CreateCommand();
                command.CommandText = $"DELETE FROM {table}";
                command.ExecuteNonQuery();
            }
        }


        static void PrintOrdersDetailed()
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT * FROM book_order o JOIN user u ON o.user_ticket = u.ticket_number";
            var reader = command.ExecuteReader();

            Console.WriteLine($"----------- Detailed Orders -----------");

            try
            {
                while (reader.Read())
                {
                    object[] values = new object[reader.FieldCount];
                    reader.GetValues(values);

                    string str = values.Aggregate((v1, v2) => $"{v1} | {v2}").ToString();
                    Console.WriteLine(str);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to print Detailed Orders\nError details: {ex.Message}");
                return;
            }
            finally
            {
                reader?.Close();
            }
        }

        static void PrintClientsCountThatHasAtLeastTwoOrders()
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM book_order o JOIN user u ON o.user_ticket = u.ticket_number GROUP BY u.id HAVING COUNT(*) >= 2";

            var reader = command.ExecuteReader();

            int rowCount = 0;
            while (reader.Read())
            {
                ++rowCount;
            }

            reader.Close();

            Console.WriteLine($"\n\nCount of users that have at least two orders: {rowCount}\n\n");
        }

        static void Main(string[] args)
        {
            CreateConnection();

            try
            {
                connection.Open();
                Console.WriteLine($"Connected: {connection.Ping()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Can't connect to database!\nError details: {ex.Message}");
                return;
            }

            //FillTables();

            foreach (String table in Tables)
            {
                PrintTable(table);
            }

            PrintClientsCountThatHasAtLeastTwoOrders();
            PrintOrdersDetailed();

        }
    }
}
