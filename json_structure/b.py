import json
from json_schema_util import generate_structure_from_object # Import the new direct function

# Your example JSON object (as a Python dictionary)
example_data = {
    "isbn": "0312853122",
    "text_reviews_count": "1", # Note: This is a string in the JSON
    "series": [],
    "country_code": "US",
    "language_code": "",
    "popular_shelves": [
        {"count": "3", "name": "to-read"},
        {"count": "1", "name": "p"},
        {"count": "1", "name": "collection"},
        {"count": "1", "name": "w-c-fields"},
        {"count": "1", "name": "biography"}
    ],
    "asin": "",
    "is_ebook": "false", # Note: This is a string in the JSON
    "average_rating": "4.00", # Note: This is a string in the JSON
    "kindle_asin": "",
    "similar_books": [],
    "description": "",
    "format": "Paperback",
    "link": "https://www.goodreads.com/book/show/5333265-w-c-fields",
    "authors": [{"author_id": "604031", "role": ""}],
    "publisher": "St. Martin's Press",
    "num_pages": "256", # Note: This is a string in the JSON
    "publication_day": "1", # Note: This is a string in the JSON
    "isbn13": "9780312853129",
    "publication_month": "9", # Note: This is a string in the JSON
    "edition_information": "",
    "publication_year": "1984", # Note: This is a string in the JSON
    "url": "https://www.goodreads.com/book/show/5333265-w-c-fields",
    "image_url": "https://images.gr-assets.com/books/1310220028m/5333265.jpg",
    "book_id": "5333265", # Note: This is a string in the JSON
    "ratings_count": "3", # Note: This is a string in the JSON
    "work_id": "5400751", # Note: This is a string in the JSON
    "title": "W.C. Fields: A Life on Film",
    "title_without_series": "W.C. Fields: A Life on Film"
}

# Generate the schema from the Python dictionary
# You can provide desired file and collection names
schema = generate_structure_from_object(
    example_data,
    file_name="goodreads_book_example.json",
    collection_name="goodreads_books"
)

if schema:
    print(json.dumps(schema, indent=2))

    # To save it to a file:
    with open("goodreads_books.json", "w", encoding='utf-8') as outfile:
        json.dump(schema, outfile, indent=2)