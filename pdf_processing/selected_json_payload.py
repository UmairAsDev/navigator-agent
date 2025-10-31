import json

# Path to the JSON file
file_path = "data/json_files/trump_tariff_tracker.json"

# Load the JSON data
with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

def country_selected_json():
    """
    Extracts and returns the 'Country_Specific_Tariffs' section from the JSON file.
    """
    # Get the 'Country_Specific_Tariffs' section
    country_tariffs = data.get('Country_Specific_Tariffs', {})
    
    # Iterate through all countries in the 'Country_Specific_Tariffs' dictionary
    for country, tariffs in country_tariffs.items():
        print(f"Country: {country}")
        
        # Process each tariff entry for the country
        for tariff in tariffs:
            type_status = tariff.get('Type_Status', {})
            ad_valorem_rate = tariff.get('Ad_Valorem_Rate', {})
            exemptions = tariff.get('Exemptions', [])
            notes = tariff.get('Notes', [])
            announced_countermeasures = tariff.get('Announced_Countermeasures', {})

            print(f"  Type_Status: {type_status}")
            print(f"  Ad_Valorem_Rate: {ad_valorem_rate}")
            print(f"  Exemptions: {exemptions}")
            print(f"  Notes: {notes}")
            print(f"  Announced_Countermeasures: {announced_countermeasures}")
            print('-' * 30)

    return country_tariffs

def product_selected_json():
    """
    Placeholder for extracting 'Product_Specific_Tariffs' if needed in the future.
    """
    product_tariffs = data.get('Product_Specific_Tariffs', {})
    
    # Iterate through all products in the 'Product_Specific_Tariffs' dictionary
    for product, tariffs in product_tariffs.items():
        print(f"Product: {product}")
        for tariff in tariffs:
            product_name = tariff.get('Product', 'N/A')
            print(f"  Product: {product_name}")

    return product_tariffs