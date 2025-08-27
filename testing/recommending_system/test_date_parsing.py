#!/usr/bin/env python3
# testing/recommending_system/test_date_parsing.py
"""
Test script to verify that date parsing works correctly with Unix date format.
"""

import pandas as pd
from datetime import datetime

def test_date_parsing():
    """Test parsing of Unix date format used in the database."""
    
    # Sample dates in the format found in your database
    sample_dates = [
        "Fri Aug 25 13:55:02 -0700 2017",
        "Mon Jan 15 09:30:45 -0800 2018", 
        "Wed Dec 12 16:22:18 -0500 2019",
        "Sun Mar 08 11:45:33 +0000 2020",
        "Thu Nov 19 14:20:15 -0600 2016"
    ]
    
    print("Testing date parsing with Unix date format...")
    print("=" * 50)
    
    # Create test DataFrame
    df = pd.DataFrame({'date_field': sample_dates})
    print("Original dates:")
    for i, date in enumerate(sample_dates):
        print(f"  {i+1}. {date}")
    
    print("\n" + "-" * 50)
    
    # Test the exact parsing logic from enhanced_offline_evaluator.py
    try:
        print("Testing UTC-aware parsing...")
        df['parsed_utc'] = pd.to_datetime(df['date_field'], errors='coerce', utc=True)
        print("✅ UTC-aware parsing successful!")
        
        # Check for any failed parsing
        failed_utc = df['parsed_utc'].isna().sum()
        if failed_utc > 0:
            print(f"⚠️ {failed_utc} dates failed UTC parsing")
        
        print("\nParsed dates (UTC):")
        for i, parsed in enumerate(df['parsed_utc']):
            print(f"  {i+1}. {parsed}")
            
    except Exception as e:
        print(f"❌ UTC parsing failed: {e}")
        
        # Fallback test
        print("\nTesting fallback parsing without UTC...")
        try:
            df['parsed_local'] = pd.to_datetime(df['date_field'], errors='coerce')
            print("✅ Fallback parsing successful!")
            
            failed_local = df['parsed_local'].isna().sum()
            if failed_local > 0:
                print(f"⚠️ {failed_local} dates failed fallback parsing")
                
            print("\nParsed dates (local):")
            for i, parsed in enumerate(df['parsed_local']):
                print(f"  {i+1}. {parsed}")
                
        except Exception as e2:
            print(f"❌ Fallback parsing also failed: {e2}")
    
    # Test temporal sorting
    print("\n" + "-" * 50)
    print("Testing temporal sorting...")
    
    if 'parsed_utc' in df.columns and not df['parsed_utc'].isna().all():
        df_sorted = df.sort_values('parsed_utc')
        print("✅ Sorted by UTC timestamps:")
    elif 'parsed_local' in df.columns and not df['parsed_local'].isna().all():
        df_sorted = df.sort_values('parsed_local')
        print("✅ Sorted by local timestamps:")
    else:
        print("❌ No valid parsed dates available for sorting")
        return False
        
    for i, row in df_sorted.iterrows():
        original = row['date_field']
        parsed = row.get('parsed_utc') or row.get('parsed_local')
        print(f"  {original} → {parsed}")
    
    # Test 80-20 split
    print("\n" + "-" * 50)
    print("Testing 80-20 temporal split...")
    
    split_idx = int(len(df_sorted) * 0.8)
    training_part = df_sorted.iloc[:split_idx]
    test_part = df_sorted.iloc[split_idx:]
    
    print(f"Total dates: {len(df_sorted)}")
    print(f"Training dates: {len(training_part)} (80%)")
    print(f"Test dates: {len(test_part)} (20%)")
    
    if len(training_part) > 0 and len(test_part) > 0:
        print("✅ Temporal split successful!")
        return True
    else:
        print("⚠️ Temporal split resulted in empty partitions")
        return False

def test_enhanced_evaluator_integration():
    """Test the date parsing as it would work in the enhanced evaluator."""
    
    print("\n" + "=" * 50)
    print("INTEGRATION TEST WITH ENHANCED EVALUATOR LOGIC")
    print("=" * 50)
    
    # Simulate a user history DataFrame with your date format
    sample_history = pd.DataFrame({
        'user_id': ['user123'] * 5,
        'book_title': ['Book A', 'Book B', 'Book C', 'Book D', 'Book E'],
        'rating': [4.5, 3.0, 5.0, 4.0, 3.5],
        'date_added': [
            "Fri Aug 25 13:55:02 -0700 2017",
            "Mon Jan 15 09:30:45 -0800 2018", 
            "Wed Dec 12 16:22:18 -0500 2019",
            "Sun Mar 08 11:45:33 +0000 2020",
            "Thu Nov 19 14:20:15 -0600 2016"
        ]
    })
    
    print("Sample user history:")
    print(sample_history.to_string(index=False))
    
    # Apply the enhanced evaluator's date parsing logic
    date_field = 'date_added'
    
    try:
        # This is the exact logic from enhanced_offline_evaluator.py
        if not pd.api.types.is_datetime64_any_dtype(sample_history[date_field]):
            try:
                sample_history[date_field] = pd.to_datetime(
                    sample_history[date_field], 
                    errors='coerce',
                    utc=True
                )
            except:
                sample_history[date_field] = pd.to_datetime(
                    sample_history[date_field], 
                    errors='coerce'
                )
        
        # Remove invalid dates
        sample_history = sample_history.dropna(subset=[date_field])
        
        if sample_history.empty:
            print("❌ All dates were invalid after parsing")
            return False
        
        # Sort by date
        sample_history = sample_history.sort_values(date_field)
        
        print(f"\n✅ Successfully parsed {len(sample_history)} dates")
        print("Sorted history:")
        print(sample_history[['book_title', 'rating', date_field]].to_string(index=False))
        
        # Test 80-20 split
        split_idx = int(len(sample_history) * 0.8)
        if split_idx < 1:
            split_idx = 1
            
        training_df = sample_history.iloc[:split_idx].copy()
        test_df = sample_history.iloc[split_idx:].copy()
        
        print(f"\n✅ Temporal split successful:")
        print(f"  Training set: {len(training_df)} books")
        print(f"  Test set: {len(test_df)} books")
        
        # Check for high-rated books in test set
        high_rated_test = test_df[test_df['rating'] >= 4.0]
        print(f"  High-rated test books: {len(high_rated_test)}")
        
        if len(high_rated_test) > 0:
            print("✅ Temporal split ready for evaluation!")
            return True
        else:
            print("⚠️ No high-rated books in test set")
            return False
            
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False

def main():
    """Run all date parsing tests."""
    print("UNIX DATE FORMAT COMPATIBILITY TEST")
    print("=" * 60)
    print("Testing compatibility with date format: 'Fri Aug 25 13:55:02 -0700 2017'")
    print()
    
    # Run basic parsing test
    basic_success = test_date_parsing()
    
    # Run integration test
    integration_success = test_enhanced_evaluator_integration()
    
    print("\n" + "=" * 60)
    print("FINAL RESULTS:")
    print(f"Basic date parsing: {'✅ PASS' if basic_success else '❌ FAIL'}")
    print(f"Integration test: {'✅ PASS' if integration_success else '❌ FAIL'}")
    
    if basic_success and integration_success:
        print("\n🎉 ALL TESTS PASSED!")
        print("Your Unix date format is fully compatible with the enhanced evaluator!")
        print("You can safely use temporal splitting in your evaluation.")
    else:
        print("\n⚠️ SOME TESTS FAILED")
        print("The enhanced evaluator will automatically fall back to random splitting.")
    
    return basic_success and integration_success

if __name__ == "__main__":
    main()