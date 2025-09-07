import pandas as pd
import numpy as np
import io
from datetime import datetime
from flask import Flask, request, send_file, render_template, jsonify

# --- Initialize Flask App ---
app = Flask(__name__)

# --- Constants & Helpers ---
SELLER_STATE = "HARYANA"
FORMATTED_SELLER_STATE = "06-Haryana"
AMAZON_GSTIN = "06AABCB2901D1CX"
AMAZON_NAME = "Amazon"
SHOPIFY_GSTIN = "24AAAAA0000A1Z5"
SHOPIFY_NAME = "Shopify"
# Flipkart GSTIN is now dynamic, so we only need the name
FLIPKART_NAME = "Flipkart"
TCS_NATURE_OF_SUPPLY = "Liable to collect tax u/s 52(TCS)"
SHEET_PASSWORD = "Sugandh@1998"

STATE_MAP = { "JAMMU & KASHMIR": "01-Jammu & Kashmir", "JAMMU AND KASHMIR": "01-Jammu & Kashmir", "HIMACHAL PRADESH": "02-Himachal Pradesh", "PUNJAB": "03-Punjab", "CHANDIGARH": "04-Chandigarh", "UTTARAKHAND": "05-Uttarakhand", "HARYANA": "06-Haryana", "DELHI": "07-Delhi", "RAJASTHAN": "08-Rajasthan", "UTTAR PRADESH": "09-Uttar Pradesh", "BIHAR": "10-Bihar", "SIKKIM": "11-Sikkim", "ARUNACHAL PRADESH": "12-Arunachal Pradesh", "NAGALAND": "13-Nagaland", "MANIPUR": "14-Manipur", "MIZORAM": "15-Mizoram", "TRIPURA": "16-Tripura", "MEGHALAYA": "17-Meghalaya", "ASSAM": "18-Assam", "WEST BENGAL": "19-West Bengal", "JHARKHAND": "20-Jharkhand", "ODISHA": "21-Odisha", "CHHATTISGARH": "22-Chhattisgarh", "MADHYA PRADESH": "23-Madhya Pradesh", "GUJARAT": "24-Gujarat", "DAMAN & DIU": "25-Daman & Diu", "DADRA & NAGAR HAVELI & DAMAN & DIU": "26-Dadra & Nagar Haveli & Daman & Diu", "MAHARASHTRA": "27-Maharashtra", "KARNATAKA": "29-Karnataka", "GOA": "30-Goa", "LAKSHDWEEP": "31-Lakshdweep", "KERALA": "32-Kerala", "TAMIL NADU": "33-Tamil Nadu", "PUDUCHERRY": "34-Puducherry", "ANDAMAN & NICOBAR ISLANDS": "35-Andaman & Nicobar Islands", "TELANGANA": "36-Telangana", "ANDHRA PRADESH": "37-Andhra Pradesh", "LADAKH": "38-Ladakh", "OTHER TERRITORY": "97-Other Territory"}

# --- Header Mapping for Flexibility ---
AMAZON_COLUMN_MAP = {
    'ship_to_state': ['ship to state', 'state'],
    'taxable_value': ['tax exclusive gross', 'taxable value'],
    'total_tax': ['total tax amount', 'total tax'],
    'igst': ['igst tax'],
    'cgst': ['cgst tax'],
    'sgst': ['sgst tax'],
    'transaction_status': ['transaction type', 'transaction status', 'order status'],
    'cess': ['compensatory cess tax', 'cess']
}

SHOPIFY_COLUMN_MAP = {
    'state': ['address state', 'shipping province', 'state'],
    'order_total': ['order total', 'total'],
    'financial_status': ['order status', 'status', 'fulfillment status', 'financial status'],
    'product_name': ['product name', 'item name'],
    'taxable_amount': ['taxable amount']
}

FLIPKART_COLUMN_MAP = {
    'seller_gstin': ['seller gstin'], # Mandatory for bifurcation
    'delivery_state': ["customer's delivery state"],
    'taxable_value': ["taxable value (final invoice amount -taxes)", "taxable value"],
    'igst': ["igst amount"],
    'cgst': ["cgst amount"],
    'sgst': ["sgst amount (or utgst as applicable)", "sgst amount"],
    'igst_rate': ["igst rate"],
    'cgst_rate': ["cgst rate"],
    'sgst_rate': ["sgst rate (or utgst as applicable)", "sgst rate"],
    'event_type': ["event type"],
    'cess': ["luxury cess amount"]
}

def get_formatted_state(state_name):
    if isinstance(state_name, str) and '-' in state_name:
        state_name = state_name.split('-')[1]
        state_abbr_map = {'KA': 'Karnataka', 'DL': 'Delhi', 'WB': 'West Bengal', 'CH': 'Chandigarh', 'BH': 'Bihar'}
        state_name = state_abbr_map.get(state_name, state_name)
    return STATE_MAP.get(str(state_name).strip().upper(), state_name)

def to_numeric(series):
    return pd.to_numeric(series, errors='coerce').fillna(0)

def find_and_rename_columns(df, column_map):
    rename_dict = {}
    df_columns_lower = {col.lower().strip(): col for col in df.columns}
    for standard_name, possible_names in column_map.items():
        for possible_name in possible_names:
            if possible_name in df_columns_lower:
                rename_dict[df_columns_lower[possible_name]] = standard_name
                break
    df.rename(columns=rename_dict, inplace=True)
    mandatory_columns = [std_name for std_name in column_map if not std_name.startswith(('transaction_status', 'cess', 'financial_status', 'product_name', 'taxable_amount', 'event_type'))]
    missing_cols = [col for col in mandatory_columns if col not in df.columns]
    if missing_cols:
        missing_friendly_names = [column_map[col][0] for col in missing_cols]
        raise ValueError(f"Missing required columns: {', '.join(missing_friendly_names)}")
    return df

# --- Data Processing Functions ---
def process_amazon_data(df):
    df = find_and_rename_columns(df, AMAZON_COLUMN_MAP)
    if 'transaction_status' in df.columns:
        NEGATIVE_STATUSES = ['CANCEL', 'REFUND', 'RETURN']
        pattern = '|'.join(NEGATIVE_STATUSES)
        df = df[~df['transaction_status'].str.contains(pattern, case=False, na=False)].copy()
    
    for col in ["taxable_value", "total_tax", "igst", "cgst", "sgst", "cess"]:
        if col in df.columns: df[col] = to_numeric(df[col])
    
    df['Place Of Supply'] = df['ship_to_state'].apply(get_formatted_state)
    df['Rate'] = np.where(df['taxable_value'] > 0, np.round((df['total_tax'] / df['taxable_value']) * 100, 2), 0)
    cess_sum = df['cess'].sum() if 'cess' in df.columns else 0

    b2cs_summary = df.groupby(['Place Of Supply', 'Rate']).agg(Taxable_Value=('taxable_value', 'sum')).reset_index()
    b2cs_summary['Cess_Amount'] = cess_sum
    b2cs_summary = b2cs_summary[b2cs_summary['Rate'] > 0]
    b2cs_summary['Type'] = 'OE'
    b2cs_summary['E-Commerce GSTIN'] = ''
    b2cs_summary = b2cs_summary[['Type', 'Place Of Supply', 'Rate', 'Taxable_Value', 'Cess_Amount', 'E-Commerce GSTIN']]
    
    tcs_summary = pd.DataFrame([{'Nature of Supply': TCS_NATURE_OF_SUPPLY, 'GSTIN of E-Commerce Operator': AMAZON_GSTIN, 'E-Commerce Operator Name': AMAZON_NAME, 'Net value of supplies': df['taxable_value'].sum(), 'Integrated tax': df['igst'].sum(), 'Central tax': df['cgst'].sum(), 'State/UT tax': df['sgst'].sum(), 'Cess': cess_sum}])
    return b2cs_summary, tcs_summary

def process_shopify_data(df):
    df = find_and_rename_columns(df, SHOPIFY_COLUMN_MAP)
    if 'product_name' in df.columns:
        df = df.dropna(subset=['product_name']).copy()
    if 'financial_status' in df.columns:
        NEGATIVE_STATUSES = ['CANCELLED', 'REFUNDED', 'VOIDED', 'RETURNED']
        pattern = '|'.join(NEGATIVE_STATUSES)
        df = df[~df['financial_status'].str.contains(pattern, case=False, na=False)].copy()
    
    df['order_total'] = to_numeric(df['order_total'])
    df['Place Of Supply'] = df['state'].apply(get_formatted_state)
    if 'taxable_amount' in df.columns:
        df['taxable_amount'] = to_numeric(df['taxable_amount'])
    else:
        df['taxable_amount'] = np.round(df['order_total'] / 1.18, 2)
    
    df['Tax Value'] = np.round(df['order_total'] - df['taxable_amount'], 2)
    df['Rate'] = np.where(df['taxable_amount'] > 0, np.round((df['Tax Value'] / df['taxable_amount']) * 100, 2), 0)
    
    is_intra_state = df['Place Of Supply'] == FORMATTED_SELLER_STATE
    df['IGST'] = np.where(is_intra_state, 0, df['Tax Value'])
    df['CGST'] = np.where(is_intra_state, df['Tax Value'] / 2, 0)
    df['SGST'] = np.where(is_intra_state, df['Tax Value'] / 2, 0)

    b2cs_summary = df.groupby(['Place Of Supply', 'Rate']).agg(Taxable_Value=('taxable_amount', 'sum')).reset_index()
    b2cs_summary['Cess_Amount'] = 0.0
    b2cs_summary = b2cs_summary[b2cs_summary['Rate'] > 0]
    b2cs_summary['Type'] = 'OE'
    b2cs_summary['E-Commerce GSTIN'] = ''
    b2cs_summary = b2cs_summary[['Type', 'Place Of Supply', 'Rate', 'Taxable_Value', 'Cess_Amount', 'E-Commerce GSTIN']]

    tcs_summary = pd.DataFrame([{'Nature of Supply': TCS_NATURE_OF_SUPPLY, 'GSTIN of E-Commerce Operator': SHOPIFY_GSTIN, 'E-Commerce Operator Name': SHOPIFY_NAME, 'Net value of supplies': df['taxable_amount'].sum(), 'Integrated tax': df['IGST'].sum(), 'Central tax': df['CGST'].sum(), 'State/UT tax': df['SGST'].sum(), 'Cess': 0.0}])
    return b2cs_summary, tcs_summary

def process_flipkart_data(df):
    df = find_and_rename_columns(df, FLIPKART_COLUMN_MAP)
    
    # This function now returns a list of summaries, one for each GSTIN
    all_summaries = []
    
    # Group data by the seller's GSTIN
    for gstin, group_df in df.groupby('seller_gstin'):
        df_gstin = group_df.copy() # Work on a copy for safety
        
        if 'event_type' in df_gstin.columns:
            NEGATIVE_STATUSES = ['RETURN', 'CANCEL']
            pattern = '|'.join(NEGATIVE_STATUSES)
            df_gstin = df_gstin[~df_gstin['event_type'].str.contains(pattern, case=False, na=False)].copy()

        for col in ["taxable_value", "igst", "cgst", "sgst", "cess", "igst_rate", "cgst_rate", "sgst_rate"]:
            if col in df_gstin.columns: df_gstin[col] = to_numeric(df_gstin[col])
        
        df_gstin['Place Of Supply'] = df_gstin['delivery_state'].apply(get_formatted_state)
        df_gstin['Rate'] = df_gstin['igst_rate'] + df_gstin['cgst_rate'] + df_gstin['sgst_rate']
        
        cess_sum = df_gstin['cess'].sum() if 'cess' in df_gstin.columns else 0

        b2cs_summary = df_gstin.groupby(['Place Of Supply', 'Rate']).agg(Taxable_Value=('taxable_value', 'sum')).reset_index()
        b2cs_summary['Cess_Amount'] = cess_sum
        b2cs_summary = b2cs_summary[b2cs_summary['Rate'] > 0]
        b2cs_summary['Type'] = 'OE'
        b2cs_summary['E-Commerce GSTIN'] = ''
        b2cs_summary = b2cs_summary[['Type', 'Place Of Supply', 'Rate', 'Taxable_Value', 'Cess_Amount', 'E-Commerce GSTIN']]

        tcs_summary = pd.DataFrame([{'Nature of Supply': TCS_NATURE_OF_SUPPLY, 
                                     'GSTIN of E-Commerce Operator': gstin, # Use the dynamic GSTIN from the group
                                     'E-Commerce Operator Name': FLIPKART_NAME, 
                                     'Net value of supplies': df_gstin['taxable_value'].sum(), 
                                     'Integrated tax': df_gstin['igst'].sum(), 
                                     'Central tax': df_gstin['cgst'].sum(), 
                                     'State/UT tax': df_gstin['sgst'].sum(), 
                                     'Cess': cess_sum}])
        
        # Append the results for this specific GSTIN
        all_summaries.append({'gstin': gstin, 'b2cs': b2cs_summary, 'tcs': tcs_summary})
        
    return all_summaries

def write_excel_sheet(writer, sheet_name, df, is_tcs=False):
    df.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=1)
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    unlocked_format = workbook.add_format({'locked': False})
    money_format = workbook.add_format({'num_format': '#,##0.00', 'locked': False})
    sig_format = workbook.add_format({'italic': True, 'font_size': 9, 'font_color': '#646464', 'align': 'right', 'locked': True})
    if is_tcs:
        header_format = workbook.add_format({'bold': True, 'font_color': 'black', 'bg_color': '#FFE6CC', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'locked': False})
        signature_col_index = 7
    else:
        header_format = workbook.add_format({'bold': True, 'font_color': 'white', 'bg_color': '#0070C0', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'locked': False})
        signature_col_index = 5
    worksheet.set_column('A:XFD', None, unlocked_format)
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
    for idx, col_name in enumerate(df.columns):
        max_len = len(str(col_name))
        if not df.empty: max_len = max(df[col_name].astype(str).map(len).max(), len(str(col_name)))
        worksheet.set_column(idx, idx, max_len + 2)
    if not df.empty:
        if is_tcs: worksheet.set_column('D:H', None, money_format)
        else: worksheet.set_column('D:E', None, money_format)
    signature_row = len(df) + 3
    worksheet.write(signature_row, signature_col_index, "Report Generated by Sugandh Mishra - Automated GSTR Tool", sig_format)
    protection_options = {'autofilter': True, 'sort': True, 'format_cells': True, 'format_columns': True, 'format_rows': True, 'insert_rows': True, 'delete_rows': True, 'objects': True, 'scenarios': True}
    worksheet.protect(SHEET_PASSWORD, protection_options)

# --- Flask Routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    amazon_file = request.files.get('amazon_file')
    shopify_file = request.files.get('shopify_file')
    flipkart_file = request.files.get('flipkart_file')

    if not amazon_file and not shopify_file and not flipkart_file:
        return jsonify({"error": "Please upload at least one report file."}), 400

    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if amazon_file and amazon_file.filename != '':
                try:
                    df_amazon = pd.read_csv(amazon_file) if amazon_file.filename.lower().endswith('.csv') else pd.read_excel(amazon_file)
                    b2cs_amazon, tcs_amazon = process_amazon_data(df_amazon)
                    write_excel_sheet(writer, "B2CS_Summary_Amazon", b2cs_amazon, is_tcs=False)
                    write_excel_sheet(writer, "TCS_Summary_Amazon", tcs_amazon, is_tcs=True)
                except Exception as e: raise ValueError(f"Error processing Amazon file: {e}")

            if shopify_file and shopify_file.filename != '':
                try:
                    df_shopify = pd.read_csv(shopify_file) if shopify_file.filename.lower().endswith('.csv') else pd.read_excel(shopify_file)
                    b2cs_shopify, tcs_shopify = process_shopify_data(df_shopify)
                    write_excel_sheet(writer, "B2CS_Summary_Shopify", b2cs_shopify, is_tcs=False)
                    write_excel_sheet(writer, "TCS_Summary_Shopify", tcs_shopify, is_tcs=True)
                except Exception as e: raise ValueError(f"Error processing Shopify file: {e}")
            
            if flipkart_file and flipkart_file.filename != '':
                try:
                    df_flipkart = pd.read_csv(flipkart_file) if flipkart_file.filename.lower().endswith('.csv') else pd.read_excel(flipkart_file)
                    # This now returns a list, so we loop through it
                    flipkart_summaries = process_flipkart_data(df_flipkart)
                    for summary in flipkart_summaries:
                        gstin = summary['gstin']
                        b2cs_summary = summary['b2cs']
                        tcs_summary = summary['tcs']
                        # Create unique sheet names for each GSTIN
                        write_excel_sheet(writer, f"B2CS_Flipkart_{gstin}", b2cs_summary, is_tcs=False)
                        write_excel_sheet(writer, f"TCS_Flipkart_{gstin}", tcs_summary, is_tcs=True)
                except Exception as e: raise ValueError(f"Error processing Flipkart file: {e}")
        
        output.seek(0)
        timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        filename = f"GSTR1_Reports_{timestamp}.xlsx"

        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)
    except Exception as e:
        print(f"Error processing file: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)