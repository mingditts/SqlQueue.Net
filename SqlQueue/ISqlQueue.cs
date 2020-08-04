using System;
using System.Data.SqlClient;

namespace SqlQueue.Core
{
	public interface ISqlQueue<T>
	{
		/// <summary>
		/// Connection string of the queue.
		/// </summary>
		string ConnectionString { get; }

		/// <summary>
		/// Schema of the queue.
		/// </summary>
		string Schema { get; }

		/// <summary>
		/// Name of the queue.
		/// </summary>
		string Name { get; }

		/// <summary>
		/// Enqueue an element.
		/// If no exceptions are thrown the operation is done correctly.
		/// </summary>
		/// <param name="element"></param>
		/// <param name="transaction"></param>
		void Enqueue(T element, SqlTransaction transaction = null);

		/// <summary>
		/// Peek an item from the queue and returns the id of the returned item. If no items are available (entityPeeked == null) then null id is returned.
		/// The peek is an exclusive distribuited competing consumer. The returned id must be used in order to dequeue the item from the queue using sqlQueue.Dequeue(id).
		/// If no exceptions are thrown the operation is done correctly.
		/// </summary>
		/// <param name="entityPeeked"></param>
		/// <param name="transaction"></param>
		/// <returns></returns>
		long? Peek(out T entityPeeked, SqlTransaction transaction = null);

		/// <summary>
		/// Dequeue an element by id.
		/// If no exceptions are thrown the operation is done correctly.
		/// </summary>
		/// <param name="id"></param>
		/// <param name="transaction"></param>
		void Dequeue(long id, SqlTransaction transaction = null);

		/// <summary>
		/// Count element in queue.
		/// </summary>
		/// <returns></returns>
		int Count();

		/// <summary>
		/// Reset all item statuses.
		/// If no exceptions are thrown the operation is done correctly.
		/// </summary>
		/// <param name="from">If null it resets (set Status = 0 that means 'ToProcess') all records. If from has value it means that it will reset all records with LastOperationDateTime < from.</param>
		void ResetAllStatuses(DateTimeOffset? from);
	}
}