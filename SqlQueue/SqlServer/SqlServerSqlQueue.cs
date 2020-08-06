using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SqlQueue.Core.SqlServer
{
	public class SqlServerSqlQueue<T> : ISqlQueue<T>
	{
		public string ConnectionString { get; private set; }

		public string Schema { get; private set; }

		public string Name { get; private set; }

		private static Dictionary<string, bool> creationDictionary = new Dictionary<string, bool>();

		public SqlServerSqlQueue(string connectionString, string schema, string name, bool createIfNotExists = true)
		{
			#region Arguments check

			if (string.IsNullOrEmpty(connectionString) || string.IsNullOrWhiteSpace(connectionString))
			{
				throw new ArgumentNullException("Missing queue connectionString.");
			}

			if (string.IsNullOrEmpty(schema) || string.IsNullOrWhiteSpace(schema))
			{
				throw new ArgumentNullException("Missing schema name.");
			}

			if (string.IsNullOrEmpty(name) || string.IsNullOrWhiteSpace(name))
			{
				throw new ArgumentNullException("Missing queue name.");
			}

			#endregion

			this.ConnectionString = connectionString;

			this.Schema = schema;

			this.Name = name;

			if (createIfNotExists)
			{
				this.EnsureCreated();
			}
		}

		public void Enqueue(T element, SqlTransaction transaction = null)
		{
			if (element == null)
			{
				throw new ArgumentNullException("Try to enqueue null element.");
			}

			this.ExecuteSql(
				$"INSERT INTO [{this.Schema}].[{this.Name}] (EnqueueDateTime, Status, LastOperationDateTime, Data) VALUES (SYSDATETIMEOFFSET(), 0, SYSDATETIMEOFFSET(), '{JsonConvert.SerializeObject(element)}')",
				null,
				transaction
			);
		}

		public long? Peek(out T entityPeeked, SqlTransaction transaction = null)
		{
			long? id = null;

			var updateInfo = this.ExecuteFetch(
				$"UPDATE QUEUE SET QUEUE.Status = 1 OUTPUT (CONCAT(INSERTED.Id, '_') + INSERTED.Data) OUT FROM (SELECT TOP 1 * FROM [{this.Schema}].[{ this.Name}] WHERE Status = 0) QUEUE",
				transaction
			);

			if (updateInfo == null)
			{
				entityPeeked = default;
				return (long?)null;
			}

			id = updateInfo.Item1;
			entityPeeked = updateInfo.Item2;

			return id;
		}

		public void Dequeue(long id, SqlTransaction transaction = null)
		{
			this.ExecuteSql(
				$"DELETE FROM [{this.Schema}].[{this.Name}] WHERE Id = @ID",
				new List<SqlParameter>() {
					new SqlParameter {
						ParameterName = "@ID",
						DbType = System.Data.DbType.Int64,
						SqlDbType = System.Data.SqlDbType.BigInt,
						Value = id
					}
				},
				transaction
			);
		}

		public int Count()
		{
			return this.ExecuteScalar($"SELECT COUNT(Id) FROM [{this.Schema}].[{this.Name}]", IsolationLevel.ReadUncommitted);
		}

		public void ResetAllStatuses(DateTimeOffset? from)
		{
			if (from == null)
			{
				this.ExecuteSql($"UPDATE [{this.Schema}].[{this.Name}] SET Status = 0");
			}
			else
			{
				this.ExecuteSql(
					$"UPDATE [{this.Schema}].[{this.Name}] SET Status = 0 WHERE LastOperationDateTime = @LASTOPERATIONDATETIME",
					new List<SqlParameter>() {
						new SqlParameter {
							ParameterName = "@LASTOPERATIONDATETIME",
							DbType = System.Data.DbType.DateTimeOffset,
							SqlDbType = System.Data.SqlDbType.DateTimeOffset,
							Value = from.Value
						}
					}
				);
			}
		}

		#region Private methods

		private void EnsureCreated()
		{
			lock (creationDictionary)   //if already done one time (application globally) for this connectionString, schema and queue skip it
			{
				string key = this.ConnectionString + "_" + this.Schema + "_" + this.Name;

				if (creationDictionary.ContainsKey(key) && creationDictionary[key] == true)
				{
					return;
				}
			}

			this.ExecuteSql($@"
				IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{this.Schema}' AND TABLE_NAME = '{this.Name}')
					CREATE TABLE [{this.Schema}].[{this.Name}] (
						Id BIGINT IDENTITY(1,1) PRIMARY KEY,
						EnqueueDateTime DATETIMEOFFSET NOT NULL,
						Status SMALLINT NOT NULL DEFAULT 0,
						LastOperationDateTime DATETIMEOFFSET NOT NULL,
						Data VARCHAR(MAX) NOT NULL
					)
			");

			lock (creationDictionary)   //set as done for this connectionString, schema and queue
			{
				string key = this.ConnectionString + "_" + this.Schema + "_" + this.Name;

				if (creationDictionary.ContainsKey(key))
				{
					creationDictionary[key] = true;
				}
				else
				{
					creationDictionary.Add(key, true);
				}
			}
		}

		private Tuple<Int64, T> ExecuteFetch(string sql, SqlTransaction transaction = null)
		{
			Tuple<Int64, T> info = null;

			if (transaction == null)
			{
				using (var sqlConnection = new SqlConnection(this.ConnectionString))
				{
					sqlConnection.Open();

					using (var sqlCommand = new SqlCommand(sql, sqlConnection, transaction))
					{
						string outInfo = (string)sqlCommand.ExecuteScalar();

						if (outInfo == null)
						{
							return null;
						}

						var pieces = outInfo.Split(new[] { '_' }, 2);

						info = new Tuple<long, T>(Convert.ToInt64(pieces[0]), JsonConvert.DeserializeObject<T>(pieces[1]));
					}
				}
			}
			else
			{
				using (var sqlCommand = new SqlCommand(sql, transaction.Connection, transaction))
				{
					string outInfo = (string)sqlCommand.ExecuteScalar();

					if (outInfo == null)
					{
						return null;
					}

					var pieces = outInfo.Split(new[] { '_' }, 2);

					info = new Tuple<long, T>(Convert.ToInt64(pieces[0]), JsonConvert.DeserializeObject<T>(pieces[1]));
				}
			}

			return info;
		}

		private int ExecuteScalar(string sql, IsolationLevel isolationLevel)
		{
			using (var sqlConnection = new SqlConnection(this.ConnectionString))
			{
				sqlConnection.Open();

				using (var transaction = sqlConnection.BeginTransaction(isolationLevel))
				{
					using (var sqlCommand = new SqlCommand(sql, sqlConnection, transaction))
					{
						return (int)sqlCommand.ExecuteScalar();
					}
				}
			}
		}

		private void ExecuteSql(string sql, List<SqlParameter> parameters = null, SqlTransaction transaction = null)
		{
			if (transaction == null)
			{
				using (var sqlConnection = new SqlConnection(this.ConnectionString))
				{
					sqlConnection.Open();

					using (var sqlCommand = new SqlCommand(sql, sqlConnection, transaction))
					{
						if (parameters != null)
						{
							sqlCommand.Parameters.AddRange(parameters.ToArray());
						}

						sqlCommand.ExecuteNonQuery();
					}
				}
			}
			else
			{
				using (var sqlCommand = new SqlCommand(sql, transaction.Connection, transaction))
				{
					if (parameters != null)
					{
						sqlCommand.Parameters.AddRange(parameters.ToArray());
					}

					sqlCommand.ExecuteNonQuery();
				}
			}
		}

		#endregion
	}
}
