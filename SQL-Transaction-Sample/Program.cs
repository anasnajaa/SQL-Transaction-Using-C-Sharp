using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace SqlTransactionSample
{
    class Program
    {
        // Data Access level ----------------------------------------------------------
        public class Parameter
        {
            private string _parameterName;
            private SqlDbType _parameterType;
            private string _parameterDefaultValue;
            public Parameter() { }
            public Parameter(string parameterName, SqlDbType parameterType, string parameterDefaultValue)
            {
                _parameterName = parameterName;
                _parameterType = parameterType;
                _parameterDefaultValue = parameterDefaultValue;
            }
            public Parameter(string parameterName, string parameterDefaultValue)
            {
                _parameterName = parameterName;
                _parameterDefaultValue = parameterDefaultValue;
            }
            public string ParameterName
            {
                set => _parameterName = ParameterName;
                get => _parameterName;
            }
            public SqlDbType ParameterType
            {
                set => _parameterType = ParameterType;
                get => _parameterType;
            }
            public string ParameterDefaultValue
            {
                set => _parameterDefaultValue = ParameterDefaultValue;
                get => _parameterDefaultValue;
            }
        }

        public static object StringAsJsonFromSqlCommand_Transaction(SqlCommand cmd, SqlConnection sqlConnection)
        {
            cmd.CommandTimeout = 0;
            cmd.Connection = sqlConnection;
            var reader = cmd.ExecuteReader();

            var result = "";

            while (reader.Read())
            {
                result += reader[0].ToString();
            }

            reader.Close();

            var jss = new System.Web.Script.Serialization.JavaScriptSerializer() { MaxJsonLength = int.MaxValue };

            return jss.DeserializeObject(result);
        }

        public static object GetStringAsJsonFromSqlCommand_UnAuthenticated_Transaction(SqlCommand cmd, SqlConnection sqlConnection)
        {
            return StringAsJsonFromSqlCommand_Transaction(cmd, sqlConnection);
        }

        // ----------------------------------------------------------

        // Utility level ----------------------------------------------------------
        public static SqlCommand SetSqlCommandWithRollBack(List<Parameter> parameters, string commandText, SqlTransaction transaction)
        {
            var sqlCommand = new SqlCommand(commandText, null, transaction);

            foreach (var t in parameters)
            {
                sqlCommand.Parameters.Add(t.ParameterName, t.ParameterType);

                object parameterValue = t.ParameterDefaultValue;

                sqlCommand.Parameters[t.ParameterName].Value = parameterValue;
            }
            return sqlCommand;
        }

        public static SqlCommand NewSqlCommandWithRollBack(string commandText, List<Parameter> parametersList, SqlTransaction transaction)
        {
            return SetSqlCommandWithRollBack(parametersList, commandText, transaction);
        }

        public static JObject JobjectFromString(object result)
        {
            return JObject.Parse(JsonConvert.SerializeObject(result));
        }

        // ----------------------------------------------------------

        static JObject FirstQuery(SqlTransaction transaction, SqlConnection connection)
        {
            const string sqlScript = @"
                    SELECT 1 AS [Status],
                    (
                        SELECT * 
                        FROM Users 
                        WHERE User_ID = @userId 
                        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
                    ) AS [User]
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
                    ";

            var sqlCommand = NewSqlCommandWithRollBack(sqlScript,
                new List<Parameter>
                {
                    new Parameter("@userId", SqlDbType.Int, "1"),
                }, transaction);

            var stringResponse = GetStringAsJsonFromSqlCommand_UnAuthenticated_Transaction(sqlCommand, connection);

            return JobjectFromString(stringResponse);
        }

        static JObject SecondQuery(SqlTransaction transaction, SqlConnection connection)
        {
            const string sqlScript = @"
                    SELECT 1 AS [Status],
                    (
                        SELECT * 
                        FROM Users 
                        WHERE User_ID = @userId 
                        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
                    ) AS [User]
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
                    ";

            var sqlCommand = NewSqlCommandWithRollBack(sqlScript,
                new List<Parameter>
                {
                    new Parameter("@userId", SqlDbType.Int, "1"),
                }, transaction);

            var stringResponse = GetStringAsJsonFromSqlCommand_UnAuthenticated_Transaction(sqlCommand, connection);

            return JobjectFromString(stringResponse);
        }

        static void Main(string[] args)
        {
            var connection = new SqlConnection("Data Source=SERVER_NAME;Initial Catalog=DB_NAME;Integrated Security=True");
            connection.Open();
            var transaction = connection.BeginTransaction();
            var firstResponse = new JObject();
            var secondResponse = new JObject();

            try
            {
                firstResponse = FirstQuery(transaction, connection);

                // inspect first response, verify that the results ok

                secondResponse = SecondQuery(transaction, connection);

                // inspect second response, verify that the results ok

                // all queries executed correctly?
                // commit transaction
                transaction.Commit();

                // otherwise roll back
                //transaction.Rollback();

                connection.Close();

                Console.WriteLine($"fr: {firstResponse}");
                Console.WriteLine($"sr: {secondResponse}");
                Console.ReadLine();
            }
            catch (Exception)
            {
                // rollback if there is an exception
                transaction.Rollback();
                connection.Close();

                Console.WriteLine($"fr: {firstResponse}");
                Console.WriteLine($"sr: {secondResponse}");
                Console.ReadLine();
            }
        }
    }
}
