import google.generativeai as genai
import os
import json
from dotenv import load_dotenv
load_dotenv()
import re
import time

# --- Configuration ---
API_KEY = os.getenv("GOOGLE_API_KEY", "")

PDF_FILE_PATH = "pdf_processor/pdfs/Trump 2.0 tariff tracker _ Trade Compliance Resource Hub.pdf"
OUTPUT_JSON_FILE = "tariff_data_from_gemini.json"
MODEL_NAME = "gemini-2.5-pro" 

SYSTEM_PROMPT = """
Convert the data in this pdf into the json format.
There are currently four sections in this pdf, Country Specific Tariffs, Product Specific Tariffs, Reciprocal Tariff Exceptions, Tariff Stacking, Updates and relevant Publications.

I want all data in these sections in form of json.
Also the first 2 sections are in the form of table.
Country Specific table have columns Country, Type & Status, Ad Valorem Rate, Exemptions & Notes, Announced Countermeasures.

Product Specific table have columns Product, Status, Ad Valorem Rate, Scope, Exemptions & Notes.

While last 3 sections are in the form of bullet point paragraphs.
Example json format that should be produced at the end:

{
  "Country_Specific_Tariffs": {
    "China": [
      {
        "Type_Status": "Trade Deal Reciprocal: Delayed until Nov. 10 (effective Aug. 11, 2025)",
        "Ad_Valorem_Rate": "34%",
        "Exemptions_Notes": "Goods from China (including Hong Kong and Macau)...",
        "Announced_Countermeasures": "Implemented 10–15% tariffs on certain U.S.-origin goods..."
      }
    ]
  }
}
"""

# --- Helper Functions ---
def configure_api(api_key):
    """Configures the generative AI API with the provided key."""
    # --- MODIFIED: Checks for an empty key ---
    if not api_key:
        print("="*50)
        print("ERROR: Please provide your Google AI Studio API Key.")
        print("You can set it as an environment variable 'GEMINI_API_KEY' or")
        print("in a .env file.")
        print("="*50)
        return None
    
    try:
        genai.configure(api_key=api_key) # type: ignore
        return True
    except Exception as e:
        print(f"Error configuring API: {e}")
        return None

def upload_file(pdf_path):
    """
    Uploads the PDF file to the Gemini API and returns a file handle.
    """
    print(f"Uploading file: {pdf_path}...")
    try:
        pdf_file = genai.upload_file(path=pdf_path) # type: ignore
        
        while pdf_file.state.name == "PROCESSING":
            print('.', end='', flush=True)
            time.sleep(10)
            pdf_file = genai.get_file(pdf_file.name) # type: ignore

        if pdf_file.state.name == "FAILED":
            print("Error: File processing failed.")
            return None

        print("\nFile uploaded successfully.")
        return pdf_file
    except Exception as e:
        print(f"Error uploading file: {e}")
        return None

def clean_json_response(text_response):
    """
    Cleans the raw text response from the model if it's not valid JSON.
    (This is a fallback in case response_mime_type fails)
    """
    match = re.search(r"```json\s*([\s\S]*?)\s*```", text_response)
    
    if match:
        json_string = match.group(1)
    else:
        json_string = text_response.strip()

    return json_string

def generate_json_from_pdf(pdf_file_handle, prompt, model_name):
    """
    Sends the prompt and the uploaded PDF to the model to generate the JSON.
    """
    print(f"Initializing model '{model_name}'...")
    
    response = None
    
    try:
        generation_config = {
            "temperature": 0.0,
            "response_mime_type": "application/json", # Request JSON output
        }
        
        # --- MODIFIED: Removed unnecessary try/except for gen_config ---
        # The model accepts the dictionary directly.
        model = genai.GenerativeModel( # type: ignore
            model_name=model_name,
            generation_config=generation_config # pyright: ignore[reportArgumentType]
        )

        print("Sending prompt and PDF to the model. This may take a moment...")
        response = model.generate_content([prompt, pdf_file_handle])
        
        print("Model response received.")
        
        if not response.text:
            print("Error: No text content received from the model.")
            return None

        return json.loads(response.text)

    except json.JSONDecodeError:
        print("Error: Model did not return valid JSON. Cleaning raw text...")
        if response and response.text:
            json_string = clean_json_response(response.text)
            try:
                return json.loads(json_string)
            except Exception as e:
                print(f"Failed to parse even the cleaned text: {e}")
                print("\n--- RAW RESPONSE (for debugging) ---")
                print(response.text)
                print("--------------------------------------")
                return None
        return None
        
    except Exception as e:
        print(f"Error during model generation: {e}")
        if response and response.text:
            print("\n--- RAW RESPONSE (for debugging) ---")
            print(response.text)
            print("--------------------------------------")
        return None

def save_json_file(data, output_path):
    """Saves the Python dictionary as a JSON file."""
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Successfully saved JSON data to {output_path}")
    except Exception as e:
        print(f"Error saving JSON file: {e}")

# --- Main Execution ---

def main():
    """Main function to run the entire PDF-to-JSON process."""
    
    print("--- Starting PDF to JSON Conversion using Gemini API ---")
    
    if not os.path.exists(PDF_FILE_PATH):
        print(f"Error: PDF file not found at '{PDF_FILE_PATH}'")
        return

    if not configure_api(API_KEY):
        return

    pdf_file = None 
    try:
        # 1. Upload the PDF
        pdf_file = upload_file(PDF_FILE_PATH)
        if not pdf_file:
            return

        # 2. Generate the JSON content
        json_data = generate_json_from_pdf(pdf_file, SYSTEM_PROMPT, MODEL_NAME)
        
        if not json_data:
            print("Process failed: No JSON data was generated.")
            return

        # 3. Save the final JSON file
        save_json_file(json_data, OUTPUT_JSON_FILE)
        
        print("\n--- Process Completed Successfully ---")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    finally:
        # Clean up the uploaded file from the API
        if pdf_file:
            print(f"Cleaning up uploaded file: {pdf_file.name}")
            genai.delete_file(pdf_file.name) # type: ignore

if __name__ == "__main__":
    main()