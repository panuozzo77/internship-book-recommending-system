
from etl.MongoDBConnection import MongoDBConnection
from core.PathRegistry import PathRegistry

def check_review_book_id_integrity_aggregation(db_name, reviews_collection_name, books_collection_name):
    """
    Checks that all `reviews.book_id` values exist in `books.book_id` using an aggregation pipeline.

    Args:
        db_name (str): The name of the MongoDB database.
        reviews_collection_name (str): The name of the reviews collection.
        books_collection_name (str): The name of the books collection.

    Returns:
        list: A list of `book_id` values from the reviews collection that
              do not exist in the books collection. Returns an empty list
              if all `book_id` values in `reviews` are present in `books`.
    """

    client = MongoDBConnection().get_client()  # Update if your connection string is different
    db = client[db_name]

    reviews_collection = db[reviews_collection_name]

    # Aggregation pipeline to find review book_ids not in books collection
    pipeline = [
        {
            "$lookup": {
                "from": books_collection_name,
                "localField": "book_id",
                "foreignField": "book_id",
                "as": "book_details"
            }
        },
        {
            "$match": {
                "book_details": {"$size": 0}  # Find reviews where book_details array is empty (no match)
            }
        },
        {
            "$project": {
                "_id": 0,
                "book_id": 1  # Only return the book_id field
            }
        }
    ]

    invalid_book_ids = []
    for result in reviews_collection.aggregate(pipeline):
        invalid_book_ids.append(result["book_id"])

    client.close() # Close the connection when done

    return invalid_book_ids

def main():
    """Main function to execute the integrity check and print results."""
    PathRegistry().set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')

    db_name = "gr_recommender" # Replace with your actual database name
    reviews_collection_name = "reviews"
    books_collection_name = "books"

    invalid_book_ids = check_review_book_id_integrity_aggregation(db_name, reviews_collection_name, books_collection_name)

    if invalid_book_ids:
        print("Integrity Check Failed:")
        print("The following book_id values from the 'reviews' collection do not exist in the 'books' collection:")
        for book_id in invalid_book_ids:
            print(f"- {book_id}")
    else:
        print("Integrity Check Passed: All book_id values in the 'reviews' collection exist in the 'books' collection.")

if __name__ == "__main__":
    main()

