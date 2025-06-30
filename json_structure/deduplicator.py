from pymongo import MongoClient, errors
from collections import defaultdict
import pymongo
from pymongo import MongoClient, UpdateOne, DeleteOne, InsertOne
import sys
import signal
from time import time
import logging

# --- Configuration ---
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "gr_recommender"  # <-- IMPORTANT: Change this to your database name
PROGRESS_COLLECTION_NAME = "dedup_progress_log"
# Process this many work_id groups before sending a bulk command to the DB.
# Adjust based on your system's memory. 500-1000 is a good starting point.
BATCH_SIZE = 500

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Graceful Shutdown Handler ---
shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    if not shutdown_requested:
        logger.info("\n--- Shutdown requested! ---")
        logger.info("The script will finish the current BATCH and then exit gracefully.")
        logger.info("Press Ctrl+C again to force exit (not recommended).")
        shutdown_requested = True
    else:
        # Second Ctrl+C is a forceful exit
        sys.exit(1)


signal.signal(signal.SIGINT, signal_handler)

# --- Main Script ---

def execute_batch(db, book_ops, review_ops, progress_ops):
    """Executes the collected bulk operations."""
    if not book_ops and not review_ops and not progress_ops:
        return 0  # Nothing to do

    logger.info(f"  > Committing batch: {len(book_ops)} book ops, {len(review_ops)} review ops, {len(progress_ops)} progress ops...")
    start_time = time()

    # Execute book updates and deletes
    if book_ops:
        db.books.bulk_write(book_ops, ordered=False)

    # Execute review updates
    if review_ops:
        # Note: review ops are UpdateMany, which are still powerful.
        # We execute them in a bulk write for consistency.
        db.reviews.bulk_write(review_ops, ordered=False)

        # Execute progress log inserts
    if progress_ops:
        db.dedup_progress_log.bulk_write(progress_ops, ordered=False)

    end_time = time()
    logger.info(f"  > Batch committed in {end_time - start_time:.2f} seconds.")

    return len(progress_ops)


def deduplicate_books_fast():
    """
    Safely and QUICKLY finds and merges duplicate books using bulk operations.
    """
    client = None
    total_processed_in_run = 0
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        books_collection = db.books
        reviews_collection = db.reviews
        progress_collection = db[PROGRESS_COLLECTION_NAME]

        progress_collection.create_index("work_id", unique=True)
        logger.info(f"Successfully connected to MongoDB: '{DATABASE_NAME}'")

        # 1. Find all potential duplicate work_ids
        logger.info("\nStep 1: Finding all potential duplicate work_ids...")
        pipeline = [
            {"$group": {"_id": "$work_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$project": {"_id": 1}}
        ]
        all_duplicate_work_ids = {group["_id"] for group in books_collection.aggregate(pipeline)}

        # 2. Find work_ids that have ALREADY been processed
        processed_work_ids = {doc["work_id"] for doc in progress_collection.find({}, {"work_id": 1})}

        # 3. Determine the list of work_ids that still need to be processed
        work_ids_to_process = list(all_duplicate_work_ids - processed_work_ids)
        total_to_process = len(work_ids_to_process)

        if not work_ids_to_process:
            logger.info("\nNo new duplicates to process. All done!")
            return

        logger.info(f"Found {len(all_duplicate_work_ids)} total duplicate work_ids.")
        logger.info(f"Skipping {len(processed_work_ids)} already processed work_ids.")
        logger.info(f"--> Starting processing for {total_to_process} remaining work_ids in batches of {BATCH_SIZE}.\n")

        # 4. Prepare for batch processing
        book_operations = []
        review_operations = []
        progress_log_operations = []

        for i, work_id in enumerate(work_ids_to_process):
            # Fetch only the necessary fields (projection)
            duplicate_books = list(books_collection.find(
                {"work_id": work_id},
                {"_id": 0, "book_id": 1, "text_reviews_count": 1, "ratings_count": 1, "title": 1}
            ))

            if len(duplicate_books) <= 1:
                progress_log_operations.append(InsertOne({"work_id": work_id}))
                continue

            # In-memory logic to decide winner/loser and calculate sums
            book_to_preserve = max(duplicate_books, key=lambda x: int(x.get("text_reviews_count", 0) or 0))
            preserved_book_id = book_to_preserve["book_id"]

            books_to_delete_ids = []
            total_ratings = 0
            total_text_reviews = 0

            for book in duplicate_books:
                total_ratings += int(book.get("ratings_count", 0) or 0)
                total_text_reviews += int(book.get("text_reviews_count", 0) or 0)
                if book["book_id"] != preserved_book_id:
                    books_to_delete_ids.append(book["book_id"])

            # --- Collect operations instead of executing them ---
            # a) Review update operation
            if books_to_delete_ids:
                review_operations.append(UpdateOne(
                    {"book_id": {"$in": books_to_delete_ids}},
                    {"$set": {"book_id": preserved_book_id}}
                ))

            # b) Preserved book update operation
            book_operations.append(UpdateOne(
                {"book_id": preserved_book_id},
                {"$set": {"ratings_count": total_ratings, "text_reviews_count": total_text_reviews}}
            ))

            # c) Duplicate book delete operations
            for book_id_to_delete in books_to_delete_ids:
                book_operations.append(DeleteOne({"book_id": book_id_to_delete}))

            # d) Progress log insert operation
            progress_log_operations.append(InsertOne({"work_id": work_id}))

            # 5. Execute the batch if it's full or if shutdown is requested
            if len(progress_log_operations) >= BATCH_SIZE or shutdown_requested:
                logger.info(f"Processing batch ending with work_id: {work_id} ({i + 1}/{total_to_process})")
                committed_count = execute_batch(db, book_operations, review_operations, progress_log_operations)
                total_processed_in_run += committed_count

                # Clear lists for the next batch
                book_operations.clear()
                review_operations.clear()
                progress_log_operations.clear()

            if shutdown_requested:
                break

        # 6. Execute any remaining operations in the last (potentially smaller) batch
        if not shutdown_requested:
            logger.info("Processing final batch...")
            committed_count = execute_batch(db, book_operations, review_operations, progress_log_operations)
            total_processed_in_run += committed_count

    except errors.ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
    except KeyboardInterrupt:
        logger.warning("\nForceful exit. Data for the last batch might not have been committed.")
    finally:
        if shutdown_requested:
            logger.info(f"\n--- Script stopped gracefully by user. ---")
            logger.info(f"Processed a total of {total_processed_in_run} work_ids in this run.")
            logger.info("You can run the script again to resume.")
        else:
            logger.info("\n--- Full deduplication process completed successfully! ---")
            logger.info(f"Processed a total of {total_processed_in_run} work_ids in this run.")
        if client:
            client.close()
            logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    deduplicate_books_fast()