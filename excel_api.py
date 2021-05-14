import datetime
import os
import openpyxl
import xlrd
import mysql.connector

from common import p_conn_data

stg_sett_fields = (
    "srno", "InsurerID", "TPAID", "ALNO", "ClaimNo", "PatientName", "AccountNo", "BeneficiaryBank_Name", "UTRNo",
    "BilledAmount", "SettledAmount", "TDS", "NetPayable", "Transactiondate", "DateofAdmission",
    "DateofDischarge", "cdate", "processing_time", "unique_key", "mail_id", "hospital", "POLICYNO",
    "CorporateName", "MemberID", "Diagnosis", "Discount", "Copay", "sett_table_sno")

col_mapping = {
    "bajaj": [['CLAIM NO', ["ALNO", "ClaimNo"]], ['PATIENT NAME', ["PatientName"]], ['DOA', ["DateofAdmission"]],
              ['DOD', ["DateofDischarge"]], ['GROSS AMOUNT', ["BilledAmount", "SettledAmount"]],
              ['TDS AMOUNT', ["TDS"]], ['NET AMOUNT', ["NetPayable"]], ['UTR NO', ["UTRNo"]], ["bajaj", "InsurerID"],
              ["bajaj", "TPAID"], ['TRANSFER DATE', ["Transactiondate"]]],

    "fhpl": [['Claim Id', ["ClaimNo"]], ['Uhid No', ["MemberID"]], ['PatientName', ["PatientName"]],
             ['PA No', ["ALNO"]], ['Insurer Name', ["InsurerID"]], ['Claimed Amt', ["BilledAmount"]],
             ['Settled Amt', ["SettledAmount", "NetPayable"]], ['Cheque/NEFT No', ["UTRNo"]],
             ['Cheque/NEFT Date', ["Transactiondate"]], ['DOA', ["DateofAdmission"]], ['DOD', ["DateofDischarge"]],
             ["fhpl", "TPAID"]],

    "genins": [['ClaimNo', ["ALNO", "ClaimNo"]], ['CardNo', ['MemberID']], ['PatientName', ['PatientName']],
               ['PolicyNo', ['POLICYNO']], ['Hospfrom', ['DateofAdmission']], ['Hospto', ['DateofDischarge']],
               ['Disease', ['Diagnosis']], ['Amount Auth', ['SettledAmount']], ['Amount Claimed', ['BilledAmount']],
               ['Amount Paid', ['NetPayable']], ['Chequeno', ['UTRNo']], ['ChequeDate', ['Transactiondate']],
               ['tds', ['TDS']], ["genins", "InsurerID"], ["genins", "TPAID"]],

    "icici": [['Policy Number', ['POLICYNO']], ['Name Of The Insured', ['PatientName']],
              ['Card Number/UHID', ['MemberID']], ['Co-Payment Value', ['Copay']], ['AL-Number', ['ALNO']],
              ['Claim-Amount Claimed', ['BilledAmount']], ['Claim-Sanctioned Amount', ['SettledAmount']],
              ['Claim-Disallowed Amount', ['Discount']], ['Claim-Cheque Date', ['Transactiondate']],
              ['Claim-Cheque Number', ['UTRNo']], ['Claim-TDS Amt', ['TDS']], ['Claim-Transferred Amt', ['NetPayable']],
              ['Claim Number', ['ClaimNo']], ["icici", "TPAID"]],

    "mediassist": [['PatientName', ['PatientName']], ['InsuranceCompany', ['InsurerID']],
                   ['ClaimId', ["ALNO", "ClaimNo"]], ['DOA', ['DateofAdmission']], ['DOD', ['DateofDischarge']],
                   ['ClaimAmount', ['BilledAmount']], ['ClaimApprovedAmt', ['SettledAmount']],
                   ['ClaimNetPayAmount', ['NetPayable']], ['SettlementDate', ['Transactiondate']],
                   ['ClaimChequeNumber', ['UTRNo']], ['Tds', ['TDS']], ["mediassist", "TPAID"]],

    "paramount": [['PHM', ['MemberID']], ['NAME_OF_INSURANCE_CO', ['InsurerID']], ['FIR', ["ALNO", "ClaimNo"]],
                  ['NAME', ['PatientName']], ['HOSPITAL_BILL_AMT', ['BilledAmount']], ['DISC_AMT', ['Discount']],
                  ['TDS_AMT', ['TDS']], ['AMOUNT_PAID', ['NetPayable']], ['DT_OF_PAYMENT', ['Transactiondate']],
                  ['DT_OF_ADMISSION', ['DateofAdmission']], ['DT_OF_DISCHARGE', ['DateofDischarge']],
                  ['UTR_NO', ['UTRNo']], ["paramount", "TPAID"]]
}

def get_data_dict(col_mapping, row, tpa_id):
    mapping = col_mapping[tpa_id]
    data_dict = {}
    for i, j in mapping:
        if isinstance(j, list):
            for k in j:
                data_dict[k] = row[i].strip()
        if isinstance(j, str):
            data_dict[j] = i.strip()
    if tpa_id == 'fhpl':
        data_dict['ClaimNo'] = data_dict['ClaimNo'].split('/')[0]
    return data_dict


def get_data(excel_file):
    _, ext = os.path.splitext(excel_file)
    data = []
    if ext == '.xlsx':
        wb_obj = openpyxl.load_workbook(excel_file)
        sheet_obj = wb_obj.active
        table = []
        for cnt, i in enumerate(sheet_obj.rows):
            row = []
            for cell in i:
                row.append(str(cell.value))
            table.append(row)
        fields = table[0]
        for row in table[1:]:
            tmp = {}
            for k, v in zip(fields, row):
                tmp[k] = v
            data.append(tmp)

    if ext == '.xls':
        wb_obj = xlrd.open_workbook(excel_file)
        sheet_obj = wb_obj.sheet_by_index(0)
        table = []
        for row in range(sheet_obj.nrows):
            tmp = []
            for cell in sheet_obj.row(row):
                tmp.append(str(cell.value))
            table.append(tmp)
        fields = table[0]
        for row in table[1:]:
            tmp = {}
            for k, v in zip(fields, row):
                tmp[k] = v
            data.append(tmp)
    return data


def date_formatting(date):
    # d b m Y
    # 30/12/2121
    # 30-12-3202
    # 30-Feb-1232
    # 12 Feb 2021
    # 12 Feb 21
    formats = ['%d/%m/%Y', '%d-%m-%Y', '%d-%b-%Y', '%d-%b-%y', '%d %b %Y', '%d %b %y']
    date = date.replace('00:00:00', '')
    date = date.strip()
    for i in formats:
        try:
            date = datetime.datetime.strptime(date, i)
            date = date.strftime('%d/%m/%Y')
            break
        except:
            pass
    return date


def ins_upd_data(datadict):
    for i in stg_sett_fields:
        if i not in datadict:
            datadict[i] = ""

    datadict['Transactiondate'] = date_formatting(datadict['Transactiondate'])
    datadict['DateofAdmission'] = date_formatting(datadict['DateofAdmission'])
    datadict['DateofDischarge'] = date_formatting(datadict['DateofDischarge'])

    q = "insert into stgSettlement_copy (`unique_key`, `InsurerID`, `TPAID`, `ALNO`, `ClaimNo`, `PatientName`, " \
        "`AccountNo`, `BeneficiaryBank_Name`, `UTRNo`, `BilledAmount`, `SettledAmount`, `TDS`, `NetPayable`, " \
        "`Transactiondate`, `DateofAdmission`, `DateofDischarge`, `mail_id`, `hospital`, `POLICYNO`, " \
        "`CorporateName`, `MemberID`, `Diagnosis`, `Discount`, `Copay`, `sett_table_sno`)"
    q = q + ' values (' + ('%s, ' * q.count(',')) + '%s) '

    params = [datadict['unique_key'], datadict['InsurerID'], datadict['TPAID'], datadict['ALNO'], datadict['ClaimNo'],
              datadict['PatientName'], datadict['AccountNo'], datadict['BeneficiaryBank_Name'], datadict['UTRNo'],
              datadict['BilledAmount'], datadict['SettledAmount'], datadict['TDS'], datadict['NetPayable'],
              datadict['Transactiondate'], datadict['DateofAdmission'], datadict['DateofDischarge'],
              datadict['mail_id'], datadict['hospital'], datadict['POLICYNO'], datadict['CorporateName'],
              datadict['MemberID'], datadict['Diagnosis'], datadict['Discount'], datadict['Copay'],
              datadict['sett_table_sno']]

    q1 = "ON DUPLICATE KEY UPDATE `InsurerID`=%s, `TPAID`=%s, `ALNO`=%s, `ClaimNo`=%s, `PatientName`=%s, " \
         "`AccountNo`=%s, `BeneficiaryBank_Name`=%s, `UTRNo`=%s, `BilledAmount`=%s, `SettledAmount`=%s, " \
         "`TDS`=%s, `NetPayable`=%s, `Transactiondate`=%s, `DateofAdmission`=%s, `DateofDischarge`=%s, " \
         "`mail_id`=%s, `hospital`=%s, `POLICYNO`=%s, `CorporateName`=%s, `MemberID`=%s, `Diagnosis`=%s, " \
         "`Discount`=%s, `Copay`=%s, `sett_table_sno`=%s"
    q = q + q1

    params = params + params[1:]

    with mysql.connector.connect(**p_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, params)
        con.commit()
    return True


if __name__ == '__main__':
    path = "/home/akshay/Downloads/Excel/Paramount_01April2020-30April2021PaidClaims.xlsx"
    data = get_data(path)
    data_dict = get_data_dict(col_mapping, data[0], "paramount")
    a = ins_upd_data(data_dict)
pass