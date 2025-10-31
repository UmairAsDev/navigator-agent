import csv
from selected_json_payload import country_selected_json

def country_specific_tariff():
    # Fetch the JSON data using the provided function
    country_tariffs = country_selected_json()
    
    # Open the CSV file for writing
    file_path = "data/csv_files/country_specific_tariff.csv"
    with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
        # Define the CSV columns
        fieldnames = [
            "Country", "Type", "Status", "Date", "Effective_Date", "Ad_Valorem_Rate", "Scope", 
            "Exempt_Title", "Exempt_Content", "Notes", "Countermeasure_Status", 
            "Countermeasure_Date", "Countermeasure_Item", "Countermeasure_Rate", "Countermeasure_Scope"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write the header row
        writer.writeheader()
        
        # Iterate through each country in the JSON data
        for country, tariffs in country_tariffs.items():
            if not tariffs:  # Skip if there are no tariffs for the country
                continue
            
            for tariff in tariffs:
                type_status = tariff.get("Type_Status", {})
                ad_valorem_rate = tariff.get("Ad_Valorem_Rate", {})
                exemptions = tariff.get("Exemptions", [])
                notes = tariff.get("Notes", [])
                announced_countermeasures = tariff.get("Announced_Countermeasures", {})
                
                # Handle Ad_Valorem_Rate
                rates = ad_valorem_rate.get("Rates", [])
                if not rates:
                    rates = [{"Rate": "", "Scope": ""}]  # Default empty rate if no rates exist
                
                # Handle Exemptions
                if not exemptions:
                    exemptions = [{"Title": "", "Content": ""}]  # Default empty exemption
                
                # Handle Notes
                if not notes:
                    notes = [""]  # Default empty note
                
                # Handle Announced Countermeasures
                countermeasures = announced_countermeasures.get("Tariffs", [])
                if not countermeasures:
                    countermeasures = [{"Item": "", "Rate": "", "Scope": ""}]  # Default empty countermeasure
                
                # Generate rows for each combination of Rates, Exemptions, Notes, and Countermeasures
                for rate in rates:
                    for exemption in exemptions:
                        for note in notes:
                            for countermeasure in countermeasures:
                                writer.writerow({
                                    "Country": country,
                                    "Type": type_status.get("Type", ""),
                                    "Status": type_status.get("Status", ""),
                                    "Date": type_status.get("Date", ""),
                                    "Effective_Date": type_status.get("Effective_Date", ""),
                                    "Ad_Valorem_Rate": rate.get("Rate", ""),
                                    "Scope": rate.get("Scope", ""),
                                    "Exempt_Title": exemption.get("Title", ""),
                                    "Exempt_Content": exemption.get("Content", ""),
                                    "Notes": note,
                                    "Countermeasure_Status": announced_countermeasures.get("Status", ""),
                                    "Countermeasure_Date": announced_countermeasures.get("Date", ""),
                                    "Countermeasure_Item": countermeasure.get("Item", ""),
                                    "Countermeasure_Rate": countermeasure.get("Rate", ""),
                                    "Countermeasure_Scope": countermeasure.get("Scope", "")
                                })

if __name__ == "__main__":
    country_specific_tariff()