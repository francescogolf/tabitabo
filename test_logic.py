"""Test script to verify TABITABO core logic."""

import pandas as pd
import Levenshtein
from typing import List, Optional


def calculate_similarity(col1: str, col2: str) -> int:
    """Calculate the Levenshtein distance between two column names."""
    return Levenshtein.distance(col1.lower(), col2.lower())


def find_best_match(target_col: str, source_cols: List[str]) -> Optional[str]:
    """
    Find the best matching column from source columns for a target column.
    Returns the match only if the Levenshtein distance is <= 3.
    """
    best_match = None
    best_distance = float('inf')
    
    for source_col in source_cols:
        distance = calculate_similarity(target_col, source_col)
        if distance <= 3 and distance < best_distance:
            best_distance = distance
            best_match = source_col
    
    return best_match


def test_similarity():
    """Test the similarity calculation."""
    print("Testing similarity calculation...")
    
    # Test exact match
    assert calculate_similarity("customer_id", "customer_id") == 0
    print("✓ Exact match: customer_id == customer_id (distance: 0)")
    
    # Test 1 character difference
    assert calculate_similarity("customer_id", "customer_i") == 1
    print("✓ 1 char diff: customer_id vs customer_i (distance: 1)")
    
    # Test 3 character difference
    assert calculate_similarity("first_name", "firstname") == 1  # One underscore removed
    print("✓ Similar: first_name vs firstname (distance: 1)")
    
    # Test completely different
    distance = calculate_similarity("customer_id", "product_name")
    assert distance > 3
    print(f"✓ Different: customer_id vs product_name (distance: {distance})")
    
    print("\nAll similarity tests passed! ✓\n")


def test_best_match():
    """Test the best match finding."""
    print("Testing best match finding...")
    
    source_cols = ["customer_id", "first_name", "last_name", "email", "phone_number"]
    
    # Test exact match
    match = find_best_match("customer_id", source_cols)
    assert match == "customer_id"
    print(f"✓ Exact match: customer_id -> {match}")
    
    # Test similar match
    match = find_best_match("firstname", source_cols)
    assert match == "first_name"
    print(f"✓ Similar match: firstname -> {match}")
    
    # Test no match (too different)
    match = find_best_match("product_id", source_cols)
    assert match is None
    print(f"✓ No match: product_id -> {match}")
    
    # Test no match due to distance > 3
    match = find_best_match("email_addr", source_cols)
    assert match is None  # distance from email is 5, which is > 3
    print(f"✓ No match for email_addr: {match} (distance > 3)")
    
    print("\nAll best match tests passed! ✓\n")


def test_decision_table():
    """Test the decision table creation."""
    print("Testing decision table creation...")
    
    # Create sample data
    tabi_metadata = pd.DataFrame({
        'column_name': ['customer_id', 'first_name', 'last_name', 'email'],
        'comment': ['Unique customer identifier', 'Customer first name', 'Customer last name', 'Email address']
    })
    
    tabo_metadata = pd.DataFrame({
        'column_name': ['user_id', 'firstname', 'lastname', 'email_addr', 'status'],
        'comment': ['', '', '', '', 'User status']
    })
    
    # Create decision table
    decision_data = []
    used_tabi_cols = set()
    
    for _, tabo_row in tabo_metadata.iterrows():
        tabo_col = tabo_row['column_name']
        tabo_comment = tabo_row['comment']
        
        available_tabi_cols = [col for col in tabi_metadata['column_name'].tolist() 
                               if col not in used_tabi_cols]
        best_match = find_best_match(tabo_col, available_tabi_cols)
        
        tabi_col = ''
        tabi_comment = ''
        
        if best_match:
            used_tabi_cols.add(best_match)
            tabi_col = best_match
            tabi_comment = tabi_metadata[tabi_metadata['column_name'] == best_match]['comment'].iloc[0]
        
        # Determine proposed description
        if tabo_comment:
            descrizione_proposta = tabo_comment
        elif tabi_comment:
            descrizione_proposta = tabi_comment
        else:
            descrizione_proposta = ''
        
        decision_data.append({
            'sostituire': True,
            'colonna_tabo': tabo_col,
            'comment_tabo': tabo_comment,
            'colonna_tabi': tabi_col,
            'comment_tabi': tabi_comment,
            'descrizione_proposta': descrizione_proposta
        })
    
    result_df = pd.DataFrame(decision_data)
    
    # Verify results
    assert len(result_df) == 5, f"Expected 5 rows, got {len(result_df)}"
    print(f"✓ Created decision table with {len(result_df)} rows")
    
    # Check that user_id didn't match anything (distance > 3 from customer_id)
    user_id_row = result_df[result_df['colonna_tabo'] == 'user_id'].iloc[0]
    assert user_id_row['colonna_tabi'] == ''
    print(f"✓ user_id has no match (correct)")
    
    # Check that firstname matched first_name
    firstname_row = result_df[result_df['colonna_tabo'] == 'firstname'].iloc[0]
    assert firstname_row['colonna_tabi'] == 'first_name'
    assert firstname_row['descrizione_proposta'] == 'Customer first name'
    print(f"✓ firstname matched first_name with correct description")
    
    # Check that status kept its own comment
    status_row = result_df[result_df['colonna_tabo'] == 'status'].iloc[0]
    assert status_row['descrizione_proposta'] == 'User status'
    print(f"✓ status kept its own comment (correct)")
    
    print("\nDecision Table Preview:")
    print(result_df[['colonna_tabo', 'colonna_tabi', 'descrizione_proposta']].to_string(index=False))
    
    print("\nAll decision table tests passed! ✓\n")


if __name__ == "__main__":
    print("="*60)
    print("TABITABO Core Logic Tests")
    print("="*60 + "\n")
    
    test_similarity()
    test_best_match()
    test_decision_table()
    
    print("="*60)
    print("All tests passed successfully! ✓✓✓")
    print("="*60)
