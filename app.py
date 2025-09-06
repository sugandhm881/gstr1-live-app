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
TCS_NATURE_OF_SUPPLY = "Liable to collect tax u/s 52(TCS)"
SHEET_PASSWORD = "Sugandh@1998"

STATE_MAP = { "JAMMU & KASHMIR": "01-Jammu & Kashmir", "JAMMU AND KASHMIR": "01-Jammu & Kashmir", "HIMACHAL PRADESH": "02-Himachal Pradesh", "PUNJAB": "03-Punjab", "CHANDIGARH": "04-Chandigarh", "UTTARAKHAND": "05-Uttarakhand", "HARYANA": "06-Haryana", "DELHI": "07-Delhi", "RAJASTHAN": "08-Rajasthan", "UTTAR PRADESH": "09-Uttar Pradesh", "BIHAR": "10-Bihar", "SIKKIM": "11-Sikkim", "ARUNACHAL PRADESH": "12-Arunachal Pradesh", "NAGALAND": "13-Nagaland", "MANIPUR": "14-Manipur", "MIZORAM": "15-Mizoram", "TRIPURA": "16-Tripura", "MEGHALAYA": "17-Meghalaya", "ASSAM": "18-Assam", "WEST BENGAL": "19-West Bengal", "JHARKHAND": "20-Jharkhand", "ODISHA": "21-Odisha", "CHHATTISGARH": "22-Chhattisgarh", "MADHYA PRADESH": "23-Madhya Pradesh", "GUJARAT": "24-Gujarat", "DAMAN & DIU": "25-Daman & Diu", "DADRA & NAGAR HAVELI & DAMAN & DIU": "26-Dadra & Nagar Haveli & Daman & Diu", "MAHARASHTRA": "27-Maharashtra", "KARNATAKA": "29-Karnataka", "GOA": "30-Goa", "LAKSHDWEEP": "31-Lakshdweep", "KERALA": "32-Kerala", "TAMIL NADU": "33-Tamil Nadu", "PUDUCHERRY": "34-Puducherry", "ANDAMAN & NICOBAR ISLANDS": "35-Andaman & Nicobar Islands", "TELANGANA": "36-Telangana", "ANDHRA PRADESH": "37-Andhra Pradesh", "LADAKH": "38-Ladakh", "OTHER TERRITORY": "97-Other Territory"}

# --- NEW: Header Mapping for Flexibility ---
# Define standard internal names and possible variations from source files.
AMAZON_COLUMN_MAP = {
    'ship_to_state': ['ship to state', 'state'],
    'taxable_value': ['tax exclusive gross', 'taxable value'],
    'total_tax': ['total tax amount', 'total tax'],
    'igst': ['igst tax'],
    'cgst': ['cgst tax'],
    'sgst': ['sgst tax'],
    # Optional columns
    'transaction_type': ['transaction type'],
    'cess': ['compensatory cess tax', 'cess']
}

SHOPIFY_COLUMN_MAP = {
    'state': ['address state', 'shipping province', 'state'],
    'order_total': ['order total', 'total'],
    # Optional columns
    'order_status': ['order status', 'status', 'fulfillment status'],
    'product_name': ['product name', 'item name'],
    'taxable_amount': ['taxable amount']
}

def get_formatted_state(state_name):
    return STATE_MAP.get(str(state_name).strip().upper(), state_name)

def to_numeric(series):
    return pd.to_numeric(series, errors='coerce').fillna(0)

def find_and_rename_columns(df, column_map):
    """
    Finds columns based on possible names, renames them to a standard,
    and checks for missing mandatory columns.
    """
    rename_dict = {}
    df_columns_lower = {col.lower().strip(): col for col in df.columns}

    for standard_name, possible_names in column_map.items():
        found = False
        for possible_name in possible_names:
            if possible_name in df_columns_lower:
                rename_dict[df_columns_lower[possible_name]] = standard_name
                found = True
                break
    
    df.rename(columns=rename_dict, inplace=True)
    
    # Validate that mandatory columns now exist
    mandatory_columns = [std_name for std_name, variations in column_map.items() if not std_name.startswith(('transaction_type', 'cess', 'order_status', 'product_name', 'taxable_amount'))]
    missing_cols = [col for col in mandatory_columns if col not in df.columns]
    
    if missing_cols:
        # Report the user-friendly names of the missing columns
        missing_friendly_names = [column_map[col][0] for col in missing_cols]
        raise ValueError(f"Missing required columns: {', '.join(missing_friendly_names)}")
    
    return df

# --- Data Processing Functions ---
def process_amazon_data(df):
    df = find_and_rename_columns(df, AMAZON_COLUMN_MAP)

    if 'transaction_type' in df.columns:
        df = df[df['transaction_type'].str.upper() != 'CANCEL'].copy()
    
    for col in ["taxable_value", "total_tax", "igst", "cgst", "sgst", "cess"]:
        if col in df.columns:
            df[col] = to_numeric(df[col])
    
    df['Place Of Supply'] = df['ship_to_state'].apply(get_formatted_state)
    df['Rate'] = np.where(df['taxable_value'] > 0, np.round((df['total_tax'] / df['taxable_value']) * 100, 2), 0)
    
    cess_sum = df['cess'].sum() if 'cess' in df.columns else 0

    b2cs_summary = df.groupby(['Place Of Supply', 'Rate']).agg(
        Taxable_Value=('taxable_value', 'sum')
    ).reset_index()
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
    if 'order_status' in df.columns:
        df = df[df['order_status'].str.upper() != 'CANCELLED'].copy()
    
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
        if not df.empty:
            max_len = max(df[col_name].astype(str).map(len).max(), len(str(col_name)))
        worksheet.set_column(idx, idx, max_len + 2)

    if not df.empty:
        if is_tcs:
            worksheet.set_column('D:H', None, money_format)
        else:
            worksheet.set_column('D:E', None, money_format)

    signature_row = len(df) + 3
    worksheet.write(signature_row, signature_col_index, "Report Generated by Sugandh Mishra - Automated GSTR1 B2C Tool", sig_format)

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

    if not amazon_file and not shopify_file:
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
                except Exception as e:
                    raise ValueError(f"Error processing Amazon file: {e}")

            if shopify_file and shopify_file.filename != '':
                try:
                    df_shopify = pd.read_csv(shopify_file) if shopify_file.filename.lower().endswith('.csv') else pd.read_excel(shopify_file)
                    b2cs_shopify, tcs_shopify = process_shopify_data(df_shopify)
                    write_excel_sheet(writer, "B2CS_Summary_Shopify", b2cs_shopify, is_tcs=False)
                    write_excel_sheet(writer, "TCS_Summary_Shopify", tcs_shopify, is_tcs=True)
                except Exception as e:
                    raise ValueError(f"Error processing Shopify file: {e}")
        
        output.seek(0)
        timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        filename = f"GSTR1_Reports_{timestamp}.xlsx"

        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)

    except Exception as e:
        print(f"Error processing file: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

