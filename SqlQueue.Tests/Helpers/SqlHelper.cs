using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlQueue.Tests.Helpers
{
	public class SqlCommandInfo
	{
		public string Sql { get; set; }

		public List<SqlParameter> Parameters { get; set; }
	}

	public class SqlHelper
	{
		/// <summary>
		/// Execute sql command
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		public static void QuerySingle(string connectionString, string sql)
		{
			using (var sqlConnection = new SqlConnection(connectionString))
			{
				sqlConnection.Open();

				using (var sqlCommand = new SqlCommand(sql, sqlConnection))
				{
					var reader = sqlCommand.ExecuteReader();
					reader.Close();
				}
			}
		}

		public static List<T> Query<T>(string connectionString, string sql, Func<SqlDataReader, T> mappingAction)
		{
			using (var sqlConnection = new SqlConnection(connectionString))
			{
				sqlConnection.Open();

				using (var sqlCommand = new SqlCommand(sql, sqlConnection))
				{
					var reader = sqlCommand.ExecuteReader();

					var elements = new List<T>();

					while (reader.Read())
					{
						elements.Add(mappingAction(reader));
					}

					reader.Close();

					return elements;
				}
			}
		}

		/// <summary>
		/// Execute sql command
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		public static void ExecuteSql(string connectionString, string sql, List<SqlParameter> parameters = null)
		{
			ExecuteSql(connectionString, new List<SqlCommandInfo>() { new SqlCommandInfo { Sql = sql, Parameters = parameters } });
		}

		/// <summary>
		/// Execute sql commands
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="commands"></param>
		public static void ExecuteSql(string connectionString, List<SqlCommandInfo> commands)
		{
			using (var sqlConnection = new SqlConnection(connectionString))
			{
				sqlConnection.Open();

				using (var sqlTransaction = sqlConnection.BeginTransaction())
				{
					foreach (var command in commands)
					{
						using (var sqlCommand = new SqlCommand(command.Sql, sqlConnection, sqlTransaction))
						{
							if (command.Parameters != null && command.Parameters.Count > 0)
							{
								sqlCommand.Parameters.AddRange(command.Parameters.ToArray());
							}

							sqlCommand.ExecuteNonQuery();
						}
					}

					sqlTransaction.Commit();
				}
			}
		}
	}
}
