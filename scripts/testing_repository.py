from recommender.repository import UserInteractionRepository
from etl.MongoDBConnection import MongoDBConnection
from core.PathRegistry import PathRegistry

PathRegistry().set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')
PathRegistry().set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')

user_profile_repo = UserInteractionRepository(MongoDBConnection())
# Use a user_id that is confirmed to exist in the database, like '6625532'.
user_profiles = user_profile_repo.find_interactions_by_user('7733407e73256b7e3763491cc86ab6be')

print("\nFirst 5 rows:")
print(user_profiles.head())
print("\nColumn Names:")
print(user_profiles.columns)
