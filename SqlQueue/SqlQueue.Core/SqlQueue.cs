using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlQueue.Core
{
	public class SqlQueue<T> : ISqlQueue<T>
	{
		public string ConnectionString { get; private set; }

		public string Schema { get; private set; }

		public string Name { get; private set; }

		private SqlTransaction transaction;

		private static Dictionary<string, bool> creationDictionary = new Dictionary<string, bool>();

		public SqlQueue(string connectionString, string schema, string name) : this(connectionString, schema, name, null)
		{

		}

		public SqlQueue(string connectionString, string schema, string name, SqlTransaction transaction)
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

			this.transaction = transaction;

			this.EnsureCreated();
		}

		public void Enqueue(T element)
		{
			if (element == null)
			{
				throw new ArgumentNullException("Try to enqueue null element.");
			}

			this.ExecuteSql($"INSERT INTO [{this.Schema}].[{this.Name}] (EnqueueDateTime, Status, LastOperationDateTime, Data) VALUES (SYSDATETIMEOFFSET(), 0, SYSDATETIMEOFFSET(), '{JsonConvert.SerializeObject(element)}')");
		}

		public long? Peek(out T entityPeeked)
		{
			long? id = null;
			T entity = default;

			this.ExecuteQuery(
				$"UPDATE QUEUE SET QUEUE.Status = 1 OUTPUT INSERTED.* FROM (SELECT TOP 1 * FROM [{this.Schema}].[{ this.Name}] WITH (ROWLOCK, UPDLOCK, READPAST) WHERE Status = 0) QUEUE",
				reader =>
				{
					if (reader.NextResult())
					{
						id = reader.GetInt64(0);
						entity = JsonConvert.DeserializeObject<T>(reader.GetString(4));
					}
				}
			);

			entityPeeked = entity;

			return id;
		}

		public void Dequeue(long id)
		{
			this.ExecuteSql($"DELETE FROM [{this.Schema}].[{this.Name}] WHERE Id = {id}");
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

		private SqlTransaction GetTransaction(SqlConnection sqlConnection)      //Transazione non va bene
		{
			return this.transaction ?? sqlConnection.BeginTransaction();
		}

		private void ExecuteSql(string sql, List<SqlParameter> parameters = null)
		{
			using (var sqlConnection = new SqlConnection(this.ConnectionString))
			{
				sqlConnection.Open();

				using (var sqlTransaction = this.GetTransaction(sqlConnection))
				{
					using (var sqlCommand = new SqlCommand(sql, sqlConnection, sqlTransaction))
					{
						if (parameters != null)
						{
							sqlCommand.Parameters.AddRange(parameters.ToArray());
						}
						sqlCommand.ExecuteNonQuery();
					}

					sqlTransaction.Commit();
				}
			}
		}

		private void ExecuteQuery(string sql, Action<SqlDataReader> action)
		{
			using (var sqlConnection = new SqlConnection(this.ConnectionString))
			{
				sqlConnection.Open();

				using (var sqlTransaction = this.GetTransaction(sqlConnection))
				{
					using (var sqlCommand = new SqlCommand(sql, sqlConnection, sqlTransaction))
					{
						var reader = sqlCommand.ExecuteReader();

						action(reader);

						if (!reader.IsClosed)
						{
							reader.Close();
						}
					}
				}
			}
		}

		#endregion
	}
}
