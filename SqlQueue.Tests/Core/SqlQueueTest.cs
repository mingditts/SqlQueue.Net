using Newtonsoft.Json;
using NUnit.Framework;
using SqlQueue.Core;
using SqlQueue.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Threading;

namespace SqlQueue.Tests.Core
{
	[TestFixture]
	[Description("Test of SqlQueue class.")]
	public class SqlQueueTest
	{
		private const string ConnectionString = @"Server=.\SQLEXPRESS;Database=SqlQueueTests;Integrated security=True;MultipleActiveResultSets=true";
		private const string SchemaName = "dbo";
		private readonly string QueueName = "Queue";

		public SqlQueueTest()
		{
			QueueName = QueueName + "-" + Guid.NewGuid().ToString("N").ToUpper();
		}

		[SetUp]
		public void SetUp() {
			try {
				SqlHelper.ExecuteSql(ConnectionString, $"DELETE FROM [{SchemaName}].[{QueueName}]"); } catch { }
		}

		[TearDown]
		public void TearDown()
		{
			try {
			SqlHelper.ExecuteSql(ConnectionString, $"DELETE FROM [{SchemaName}].[{QueueName}]"); } 
			
			
			catch (Exception err)
			{
			
			
			}
		}

		[OneTimeTearDown]
		public void OneTimeTearDown()
		{
			try { SqlHelper.ExecuteSql(ConnectionString, $"DROP TABLE [{SchemaName}].[{QueueName}]"); } catch (Exception err)
			{


			}
		}

		[Test]
		[Description("Tests queue creation.")]
		public void Queue_Creation_Success()
		{
			new SqlQueue<int>(ConnectionString, SchemaName, QueueName);

			new SqlQueue<int>(ConnectionString, SchemaName, QueueName);  //twice for ensure no collision

			Assert.DoesNotThrow(() => SqlHelper.QuerySingle(ConnectionString, $"SELECT * FROM [{SchemaName}].[{QueueName}]"), "Queue not created correctly.");
		}

		[Test]
		[Description("Tests item enqueue.")]
		public void Queue_Enqueue_Success()
		{
			var queue = new SqlQueue<int>(ConnectionString, SchemaName, QueueName);

			queue.Enqueue(345);

			var items = SqlHelper.Query<int>(ConnectionString, $"SELECT * FROM [{SchemaName}].[{QueueName}]", (reader) =>
			{
				return Convert.ToInt32(JsonConvert.DeserializeObject<int>(reader.GetString(4)));
			});

			Assert.AreEqual(1, items.Count, "Wrong item collection.");
			Assert.AreEqual(345, items[0], "Wrong item.");
		}

		[Test]
		[Description("Tests item peek.")]
		public void Queue_Peek_Success()
		{
			var queue = new SqlQueue<int>(ConnectionString, SchemaName, QueueName);

			queue.Enqueue(345);

			var items = SqlHelper.Query<int>(ConnectionString, $"SELECT * FROM [{SchemaName}].[{QueueName}]", (reader) =>
			{
				return Convert.ToInt32(JsonConvert.DeserializeObject<int>(reader.GetString(4)));
			});

			Assert.AreEqual(1, items.Count, "Wrong item collection.");
			Assert.AreEqual(345, items[0], "Wrong item.");
			Assert.AreEqual(1, queue.Count(), "Wrong item collection by queue count.");

			int peekedElement;

			long? id = queue.Peek(out peekedElement);

			Assert.IsNotNull(id, "Id is null after peek.");
			Assert.IsTrue(id.Value > 0, "Id <= 0");
		}

		//[Test]
		[Description("Tests random competing consumer.")]
		public void Queue_CompetingConsumer_ExclusiveFetch()
		{
			var queue = new SqlQueue<int>(ConnectionString, SchemaName, QueueName);

			Dictionary<int, int> checkDic = new Dictionary<int, int>();

			var t1 = new Thread(() =>
			{
				while (queue.Count() > 0)
				{
					int peekedElement;

					Thread.Sleep(100);

					long? id = queue.Peek(out peekedElement);

					if (id == null)
					{
						return;
					}

					lock (checkDic)
					{
						if (!checkDic.ContainsKey(peekedElement))
						{
							checkDic.Add(peekedElement, 1);
						}
						else
						{
							checkDic[peekedElement] = checkDic[peekedElement] + 1;
						}
					}

					queue.Dequeue(id.Value);
				}
			});

			var t2 = new Thread(() =>
			{
				while (queue.Count() > 0)
				{
					int peekedElement;

					Thread.Sleep(100);

					long? id = queue.Peek(out peekedElement);

					if (id == null)
					{
						return;
					}

					lock (checkDic)
					{
						if (!checkDic.ContainsKey(peekedElement))
						{
							checkDic.Add(peekedElement, 1);
						}
						else
						{
							checkDic[peekedElement] = checkDic[peekedElement] + 1;
						}
					}

					queue.Dequeue(id.Value);
				}
			});

			var t3 = new Thread(() =>
			{
				for (int i = 0; i < 10000; i++)
				{
					queue.Enqueue(i);
				}
			});

			t1.Start();
			t2.Start();

			Thread.Sleep(1000);

			t3.Start();

			t3.Join();
			t1.Join();
			t2.Join();




			int c = 0;
		}
	}
}