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
AMAZON_GSTIN = "06AABCB2901D1CX" # Kept as a default/fallback
AMAZON_NAME = "Amazon"
SHOPIFY_GSTIN = "29AAYCS6474K1Z5"
SHOPIFY_NAME = "Shopify"
FLIPKART_NAME = "Flipkart"
TCS_NATURE_OF_SUPPLY = "Liable to collect tax u/s 52(TCS)"
SHEET_PASSWORD = "Sugandh@1998"

# --- ### ROBUST STATE MAP (Full Formatting) ### ---
STATE_MAP = {
    "JAMMU AND KASHMIR": "01-Jammu & Kashmir", "LADAKH": "38-Ladakh",
    "HIMACHAL PRADESH": "02-Himachal Pradesh", "PUNJAB": "03-Punjab",
    "CHANDIGARH": "04-Chandigarh", "UTTARAKHAND": "05-Uttarakhand",
    "HARYANA": "06-Haryana", "DELHI": "07-Delhi", "RAJASTHAN": "08-Rajasthan",
    "UTTAR PRADESH": "09-Uttar Pradesh", "BIHAR": "10-Bihar", "SIKKIM": "11-Sikkim",
    "ARUNACHAL PRADESH": "12-Arunachal Pradesh", "NAGALAND": "13-Nagaland",
    "MANIPUR": "14-Manipur", "MIZORAM": "15-Mizoram", "TRIPURA": "16-Tripura",
    "MEGHALAYA": "17-Meghalaya", "ASSAM": "18-Assam", "WEST BENGAL": "19-West Bengal",
    "JHARKHAND": "20-Jharkhand", "ODISHA": "21-Odisha", "CHHATTISGARH": "22-Chhattisgarh",
    "MADHYA PRADESH": "23-Madhya Pradesh", "GUJARAT": "24-Gujarat",
    "DAMAN AND DIU": "26-Dadra & Nagar Haveli & Daman & Diu",
    "DADRA AND NAGAR HAVELI": "26-Dadra & Nagar Haveli & Daman & Diu",
    "DADRA AND NAGAR HAVELI AND DAMAN AND DIU": "26-Dadra & Nagar Haveli & Daman & Diu",
    "MAHARASHTRA": "27-Maharashtra", "GOA": "30-Goa", "KARNATAKA": "29-Karnataka",
    "LAKSHADWEEP": "31-Lakshdweep", "KERALA": "32-Kerala", "TAMIL NADU": "33-Tamil Nadu",
    "PUDUCHERRY": "34-Puducherry", "PONDICHERRY": "34-Puducherry",
    "ANDAMAN AND NICOBAR ISLANDS": "35-Andaman & Nicobar Islands",
    "ANDAMAN AND NICOBAR": "35-Andaman & Nicobar Islands", "TELANGANA": "36-Telangana",
    "ANDHRA PRADESH": "37-Andhra Pradesh", "OTHER TERRITORY": "97-Other Territory",
    "OTHER COUNTRY": "96-Other Country"
}
STATE_CODE_TO_NAME_MAP = {
    'AP': 'ANDHRA PRADESH', 'AR': 'ARUNACHAL PRADESH', 'AS': 'ASSAM', 'BR': 'BIHAR', 'CT': 'CHHATTISGARH', 'GA': 'GOA',
    'GJ': 'GUJARAT', 'HR': 'HARYANA', 'HP': 'HIMACHAL PRADESH', 'JH': 'JHARKHAND', 'KA': 'KARNATAKA', 'KL': 'KERALA',
    'MP': 'MADHYA PRADESH', 'MH': 'MAHARASHTRA', 'MN': 'MANIPUR', 'ML': 'MEGHALAYA', 'MZ': 'MIZORAM', 'NL': 'NAGALAND',
    'OR': 'ODISHA', 'PB': 'PUNJAB', 'RJ': 'RAJASTHAN', 'SK': 'SIKKIM', 'TN': 'TAMIL NADU', 'TG': 'TELANGANA',
    'TR': 'TRIPURA', 'UP': 'UTTAR PRADESH', 'UT': 'UTTARAKHAND', 'WB': 'WEST BENGAL', 'AN': 'ANDAMAN AND NICOBAR ISLANDS',
    'CH': 'CHANDIGARH', 'DN': 'DADRA AND NAGAR HAVELI', 'DD': 'DAMAN AND DIU', 'DL': 'DELHI', 'JK': 'JAMMU AND KASHMIR',
    'LA': 'LADAKH', 'LD': 'LAKSHADWEEP', 'PY': 'PUDUCHERRY'
}

def get_state_name_from_code(code_str):
    if not isinstance(code_str, str) or '-' not in code_str:
        return code_str
    abbr = code_str.split('-')[1].upper()
    return STATE_CODE_TO_NAME_MAP.get(abbr, abbr)

def get_formatted_state(state_name):
    if not isinstance(state_name, str): return state_name
    if '-' in state_name and len(state_name.split('-')[1]) == 2:
        state_name = get_state_name_from_code(state_name)
    state_abbr_map = {'KA': 'KARNATAKA', 'DL': 'DELHI', 'WB': 'WEST BENGAL', 'CH': 'CHANDIGARH', 'BH': 'BIHAR'}
    lookup_key = state_name.strip().upper()
    if lookup_key in state_abbr_map: lookup_key = state_abbr_map[lookup_key]
    lookup_key = lookup_key.replace('&', 'AND')
    return STATE_MAP.get(lookup_key, state_name)

# --- Header Mapping for Flexibility ---
AMAZON_COLUMN_MAP = {
    'seller_gstin': ['seller gstin', 'merchant gstin'], 'ship_to_state': ['ship to state', 'state'],
    'taxable_value': ['tax exclusive gross', 'taxable value'], 'total_tax': ['total tax amount', 'total tax'],
    'igst': ['igst tax'], 'cgst': ['cgst tax'], 'sgst': ['sgst tax'],
    'transaction_status': ['transaction type', 'transaction status', 'order status'], 'cess': ['compensatory cess tax', 'cess']
}
# --- update SHOPIFY_COLUMN_MAP to include shipment status ---
SHOPIFY_COLUMN_MAP = {
    'state': ['address state', 'shipping province', 'state'],
    'order_total': ['order total', 'total'],
    'financial_status': ['order status', 'status', 'fulfillment status', 'financial status'],
    'product_name': ['product name', 'item name'],
    'taxable_amount': ['taxable amount'],
    'shipment_status': ['shipment status', 'shipment_status', 'shipment status (name)', 'shipping status']
}
FLIPKART_COLUMN_MAP = {
    'seller_gstin': ['seller gstin'],
    'billing_state': ["customer's billing state"], # --- USE BILLING STATE ---
    'delivery_state': ["customer's delivery state"], 
    'shipped_from_state': ['order shipped from (state)'],
    'taxable_value': ["taxable value (final invoice amount -taxes)", "taxable value"], 'igst': ["igst amount"],
    'cgst': ["cgst amount"], 'sgst': ["sgst amount (or utgst as applicable)", "sgst amount"],
    'igst_rate': ["igst rate"], 'cgst_rate': ["cgst rate"], 'sgst_rate': ["sgst rate (or utgst as applicable)", "sgst rate"],
    'event_type': ["event type"], 'cess': ["luxury cess amount"]
}

def to_numeric(series):
    return pd.to_numeric(series, errors='coerce').fillna(0)

def find_and_rename_columns(df, column_map):
    rename_dict = {}
    df_columns_lower = {col.lower().strip(): col for col in df.columns}
    for standard_name, possible_names in column_map.items():
        for possible_name in possible_names:
            if possible_name in df_columns_lower:
                original_col_name = df_columns_lower[possible_name]
                rename_dict[original_col_name] = standard_name
                break
    df.rename(columns=rename_dict, inplace=True)
    mandatory_columns = [std for std in column_map if not std.startswith(('trans', 'cess', 'finan', 'prod', 'taxable_am', 'event', 'delivery_state'))] # delivery_state is not critical
    if 'seller_gstin' not in df.columns and 'seller_gstin' in mandatory_columns:
        mandatory_columns.remove('seller_gstin')
    missing_cols = [col for col in mandatory_columns if col not in df.columns]
    if missing_cols:
        missing_friendly_names = [column_map[c][0] for c in missing_cols]
        raise ValueError(f"Missing required columns: {', '.join(missing_friendly_names)}")
    return df

def process_amazon_data(df):
    df = find_and_rename_columns(df, AMAZON_COLUMN_MAP)
    all_summaries = []
    if 'seller_gstin' not in df.columns:
        df['seller_gstin'] = AMAZON_GSTIN
    else:
        df['seller_gstin'] = df['seller_gstin'].astype(str).str.strip().str.upper()
    financial_cols = ["taxable_value", "total_tax", "igst", "cgst", "sgst", "cess"]
    for col in financial_cols:
        if col in df.columns: df[col] = to_numeric(df[col])
    if 'transaction_status' in df.columns:
        is_refund = df['transaction_status'].str.contains('CANCEL|REFUND|RETURN', case=False, na=False)
        for col in financial_cols:
            if col in df.columns:
                df.loc[is_refund, col] = -df.loc[is_refund, col].abs()
                df.loc[~is_refund, col] = df.loc[~is_refund, col].abs()
    for gstin, group_df in df.groupby('seller_gstin'):
        df_gstin = group_df.copy()
        df_gstin['Place Of Supply'] = df_gstin['ship_to_state'].apply(get_formatted_state)
        df_gstin['Rate'] = np.where(df_gstin['taxable_value'].abs() > 0, round((df_gstin['total_tax'].abs() / df_gstin['taxable_value'].abs()) * 100, 2), 0)
        b2cs_summary = df_gstin.groupby(['Place Of Supply', 'Rate']).agg(Taxable_Value=('taxable_value', 'sum')).reset_index()
        b2cs_summary = b2cs_summary[b2cs_summary['Rate'] > 0].copy()
        b2cs_summary['Cess_Amount'] = df_gstin['cess'].sum() if 'cess' in df_gstin.columns else 0
        b2cs_summary['E-Commerce GSTIN'] = gstin
        tcs_summary = pd.DataFrame([{'Nature of Supply': TCS_NATURE_OF_SUPPLY, 'GSTIN of E-Commerce Operator': gstin,
            'E-Commerce Operator Name': AMAZON_NAME, 'Net value of supplies': df_gstin['taxable_value'].sum(),
            'Integrated tax': df_gstin['igst'].sum(), 'Central tax': df_gstin['cgst'].sum(),
            'State/UT tax': df_gstin['sgst'].sum(), 'Cess': df_gstin['cess'].sum()}])
        all_summaries.append({'gstin': gstin, 'b2cs': b2cs_summary, 'tcs': tcs_summary})
    return all_summaries

def process_shopify_data(df):
    df = find_and_rename_columns(df, SHOPIFY_COLUMN_MAP)
    if 'product_name' in df.columns: df = df.dropna(subset=['product_name']).copy()
    if 'financial_status' in df.columns:
        df = df[~df['financial_status'].str.contains('CANCELLED|REFUNDED|VOIDED|RETURNED', case=False, na=False)].copy()

    # NEW: Exclude orders based on shipment status
    excluded_shipment_statuses = {'RTO_DELIVERED', 'NA', 'RTO_INITIATED', 'CANCELLED', 'EXCEPTION', 'N/A', ''}
    if 'shipment_status' in df.columns:
        normalized_shipment_status = df['shipment_status'].fillna('NA').astype(str).str.upper().str.strip()
        df = df[~normalized_shipment_status.isin(excluded_shipment_statuses)].copy()

    df['order_total'] = to_numeric(df['order_total'])
    df['Place Of Supply'] = df['state'].apply(get_formatted_state)
    if 'taxable_amount' in df.columns: df['taxable_amount'] = to_numeric(df.get('taxable_amount'))
    else: df['taxable_amount'] = round(df['order_total'] / 1.18, 2)
    df['Tax Value'] = round(df['order_total'] - df['taxable_amount'], 2)
    df['Rate'] = np.where(df['taxable_amount'] > 0, round((df['Tax Value'] / df['taxable_amount']) * 100, 2), 0)
    is_intra_state = df['Place Of Supply'] == FORMATTED_SELLER_STATE
    df['IGST'] = np.where(is_intra_state, 0, df['Tax Value'])
    df['CGST'] = np.where(is_intra_state, df['Tax Value'] / 2, 0)
    df['SGST'] = np.where(is_intra_state, df['Tax Value'] / 2, 0)
    b2cs_summary = df.groupby(['Place Of Supply', 'Rate']).agg(Taxable_Value=('taxable_amount', 'sum')).reset_index()
    b2cs_summary = b2cs_summary[b2cs_summary['Rate'] > 0].copy()
    b2cs_summary['Cess_Amount'] = 0.0
    b2cs_summary['E-Commerce GSTIN'] = SHOPIFY_GSTIN
    tcs_summary = pd.DataFrame([{'Nature of Supply': TCS_NATURE_OF_SUPPLY, 'GSTIN of E-Commerce Operator': SHOPIFY_GSTIN,
        'E-Commerce Operator Name': SHOPIFY_NAME, 'Net value of supplies': df['taxable_amount'].sum(), 'Integrated tax': df['IGST'].sum(),
        'Central tax': df['CGST'].sum(), 'State/UT tax': df['SGST'].sum(), 'Cess': 0.0}])
    return [{'gstin': SHOPIFY_GSTIN, 'b2cs': b2cs_summary, 'tcs': tcs_summary}]

def snap_to_gst_rate(rate):
    if pd.isna(rate): return 0
    slabs = [0, 5, 12, 18, 28]
    closest_slab = min(slabs, key=lambda x: abs(x - rate))
    if abs(closest_slab - rate) < 2: return closest_slab
    return round(rate, 1)

def process_flipkart_data(df):
    df = find_and_rename_columns(df, FLIPKART_COLUMN_MAP)
    all_summaries = []
    if 'seller_gstin' in df.columns:
        df['seller_gstin'] = df['seller_gstin'].astype(str).str.strip().str.upper()
    else:
        raise ValueError("Critical error: 'seller gstin' column not found in Flipkart report.")
    financial_cols = ["taxable_value", "igst", "cgst", "sgst", "cess"]
    for col in financial_cols:
        if col in df.columns: df[col] = to_numeric(df[col])
    if 'event_type' in df.columns:
        is_refund = df['event_type'].str.contains('RETURN|CANCEL', case=False, na=False)
        for col in financial_cols:
            if col in df.columns:
                df.loc[is_refund, col] = -df.loc[is_refund, col].abs()
                df.loc[~is_refund, col] = df.loc[~is_refund, col].abs()

    for gstin, df_gstin in df.groupby('seller_gstin'):
        df_gstin = df_gstin.copy()
        
        # --- DEFINITIVE FIX: Use BILLING STATE as Place of Supply ---
        df_gstin['shipped_from_state_fmt'] = df_gstin['shipped_from_state'].apply(get_formatted_state)
        df_gstin['Place Of Supply'] = df_gstin['billing_state'].apply(get_formatted_state)
        df_gstin['is_intra'] = df_gstin['shipped_from_state_fmt'] == df_gstin['Place Of Supply']
        
        rate_cols_exist = all(c in df_gstin.columns for c in ['igst_rate', 'cgst_rate', 'sgst_rate'])
        tax_amt_cols_exist = all(c in df_gstin.columns for c in ['igst', 'cgst', 'sgst'])
        
        if rate_cols_exist:
            for col in ["igst_rate", "cgst_rate", "sgst_rate"]: df_gstin[col] = to_numeric(df_gstin[col])
            df_gstin['Rate'] = df_gstin['igst_rate'] + df_gstin['cgst_rate'] + df_gstin['sgst_rate']
        elif tax_amt_cols_exist and 'taxable_value' in df_gstin.columns:
            total_tax = df_gstin['igst'] + df_gstin['cgst'] + df_gstin['sgst']
            calculated_rate = np.where(df_gstin['taxable_value'].abs() > 0, (total_tax.abs() / df_gstin['taxable_value'].abs()) * 100, 0)
            df_gstin['Rate'] = pd.Series(calculated_rate).apply(snap_to_gst_rate)
        else:
            raise ValueError(f"Flipkart report for {gstin} must have either tax rate columns or tax amount columns.")
        
        if not tax_amt_cols_exist:
            df_gstin['total_tax'] = (df_gstin['taxable_value'] * df_gstin['Rate']) / 100
            df_gstin['igst'] = np.where(df_gstin['is_intra'] == False, df_gstin['total_tax'], 0)
            df_gstin['cgst'] = np.where(df_gstin['is_intra'] == True, df_gstin['total_tax'] / 2, 0)
            df_gstin['sgst'] = np.where(df_gstin['is_intra'] == True, df_gstin['total_tax'] / 2, 0)
            
        b2cs_summary = df_gstin.groupby(['Place Of Supply', 'Rate']).agg(Taxable_Value=('taxable_value', 'sum')).reset_index()
        b2cs_summary = b2cs_summary[(b2cs_summary['Rate'] > 0) & (b2cs_summary['Taxable_Value'] > 0)].copy()
        b2cs_summary['Cess_Amount'] = df_gstin['cess'].sum() if 'cess' in df_gstin.columns else 0
        b2cs_summary['E-Commerce GSTIN'] = gstin
        
        tcs_summary = pd.DataFrame([{'Nature of Supply': TCS_NATURE_OF_SUPPLY, 'GSTIN of E-Commerce Operator': gstin,
            'E-Commerce Operator Name': FLIPKART_NAME, 'Net value of supplies': df_gstin['taxable_value'].sum(), 'Integrated tax': df_gstin['igst'].sum(),
            'Central tax': df_gstin['cgst'].sum(), 'State/UT tax': df_gstin['sgst'].sum(), 'Cess': df_gstin['cess'].sum()}])
        all_summaries.append({'gstin': gstin, 'b2cs': b2cs_summary, 'tcs': tcs_summary})
    return all_summaries

def write_excel_sheet(writer, sheet_name, df, is_tcs=False):
    df_display = df.copy()
    if not is_tcs and 'Type' not in df_display.columns: 
        df_display.insert(0, 'Type', 'OE')
    df_display.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=1)
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    header_bg = '#FFE6CC' if is_tcs else '#0070C0'
    header_font = 'black' if is_tcs else 'white'
    header_format = workbook.add_format({'bold': True, 'font_color': header_font, 'bg_color': header_bg, 'align': 'center', 'valign': 'vcenter', 'border': 1})
    money_format = workbook.add_format({'num_format': '#,##0.00', 'locked': False})
    unlocked_format = workbook.add_format({'locked': False})
    for col_num, col_name in enumerate(df_display.columns.values):
        worksheet.write(0, col_num, col_name, header_format)
        if not df_display.empty:
            max_len = df_display[col_name].astype(str).str.len().max()
            if pd.isna(max_len): max_len = 0
        else: max_len = 0
        column_width = max(int(max_len), len(col_name)) + 3
        worksheet.set_column(col_num, col_num, column_width)
    if not df_display.empty:
        worksheet.set_column(0, len(df_display.columns) - 1, None, unlocked_format)
        money_cols = [col for col in df_display.columns if any(kw in col.lower() for kw in ['value', 'tax', 'amount', 'cess'])]
        for col_name in money_cols:
            col_idx = df_display.columns.get_loc(col_name)
            worksheet.set_column(col_idx, col_idx, None, money_format)
    sig_format = workbook.add_format({'italic': True, 'font_size': 9, 'font_color': '#646464', 'align': 'right', 'locked': True})
    sig_col_index = len(df_display.columns) - 1 if not df_display.empty else 0
    worksheet.write(len(df_display) + 3, sig_col_index, "Report Generated by Sugandh Mishra - Automated GSTR Tool", sig_format)
    worksheet.protect(SHEET_PASSWORD, {'autofilter': True, 'sort': True, 'format_columns': True})

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    try:
        files = request.files
        if not any(files.values()):
            return jsonify({"error": "Please upload at least one report file."}), 400
        
        all_b2cs, all_tcs = [], []
        excel_buffer = io.BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            file_processors = {
                'amazon_file': (process_amazon_data, "Amazon"),
                'shopify_file': (process_shopify_data, "Shopify"),
                'flipkart_file': (process_flipkart_data, "Flipkart")
            }
            num_summaries_generated = 0
            for file_key, (processor, platform_name) in file_processors.items():
                if files.get(file_key):
                    df = pd.read_csv(files[file_key]) if files[file_key].filename.lower().endswith('.csv') else pd.read_excel(files[file_key])
                    summaries = processor(df)
                    num_summaries_generated += len(summaries)
                    for summary in summaries:
                        b2cs, tcs, gstin = summary['b2cs'], summary['tcs'], summary['gstin']
                        tcs['Platform'] = platform_name
                        write_excel_sheet(writer, f"B2CS_{platform_name}_{gstin[:6]}", b2cs.drop(columns=['E-Commerce GSTIN']), is_tcs=False)
                        write_excel_sheet(writer, f"TCS_{platform_name}_{gstin[:6]}", tcs, is_tcs=True)
                        all_b2cs.append(b2cs)
                        all_tcs.append(tcs)
            
            if num_summaries_generated > 1:
                if all_b2cs:
                    combined_b2cs_df = pd.concat(all_b2cs, ignore_index=True)
                    final_b2cs_summary = combined_b2cs_df.groupby(['Place Of Supply', 'Rate']).agg(
                        Taxable_Value=('Taxable_Value', 'sum'), Cess_Amount=('Cess_Amount', 'sum')).reset_index()
                    write_excel_sheet(writer, "Combined_B2CS_Summary", final_b2cs_summary, is_tcs=False)
                if all_tcs:
                    combined_tcs_df = pd.concat(all_tcs, ignore_index=True)
                    final_tcs_summary = combined_tcs_df.groupby(['Platform', 'GSTIN of E-Commerce Operator', 'E-Commerce Operator Name']).agg({
                        'Net value of supplies': 'sum', 'Integrated tax': 'sum',
                        'Central tax': 'sum', 'State/UT tax': 'sum', 'Cess': 'sum'
                    }).reset_index()
                    write_excel_sheet(writer, "Combined_TCS_Summary", final_tcs_summary, is_tcs=True)

        excel_buffer.seek(0)
        timestamp = datetime.now().strftime('%d-%m-%Y_%H-%M')
        filename = f"GSTR1_Report_{timestamp}.xlsx"
        
        return send_file(
            excel_buffer,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        print(f"Error processing file: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)