from datetime import datetime
import json

def get_records_by_status(db_client, status = 'PROCESSED'):
    ts = datetime.now()

    date = ts.day
    month = ts.month
    year = ts.year

    date = f"0{date}" if date < 10 else date
    month = f"0{month}" if month < 10 else month

    print(f'fetching records by date: {date}-{month}-{year}')

    # fetch todays data from DB whose status is false
    db_response, date, month, year = db_client.get_data_by_date(f"{date}-{month}-{year}", status=[status])


    return db_response, (date, month, year)


def update_records(record, update_for: str, db, db_obj, api_response = {}, send_date = ''):
    if db == 'METADATA':
        if update_for == 'success':
            status = db_obj.update_item({"PK": record['PK'], "SK":'METADATA'},
                update_expression = f"set #status=:s, #message=:t, #date=:d, #payload_size=:p",
                expression_attribute_names={
                    '#status': "STATUS",
                    '#message': "MESSAGE",
                    '#date': "SEND_DATE",
                    '#payload_size': "PAYLOAD_SIZE"
                },
                expression_attribute_values={
                    ':s': str('SUBMITTED'), ':t': json.dumps(api_response), ':d': send_date, ':p': record['PAYLOAD_SIZE']})
        
        elif update_for == 'failure':
            status = db_obj.update_item({"PK": record['PK'], "SK":'METADATA'},
                update_expression = f"set #status=:s, #message=:t, #retry=:r, #date=:d, #payload_size=:p",
                expression_attribute_names={
                    '#status': "STATUS",
                    '#message': "MESSAGE",
                    '#retry': "RETRY",
                    '#date': "SEND_DATE",
                    '#payload_size': "PAYLOAD_SIZE"
                },
                expression_attribute_values={
                    ':s': str('FAILED'), ':t': json.dumps(api_response), ':r':int(record['RETRY'])+1, ':d': send_date, ':p': record['PAYLOAD_SIZE']})
            
        elif update_for == 'pending':
            status = db_obj.update_item({"PK": record['PK'], "SK":'METADATA'},
                update_expression = f"set #status=:s, #message=:t, #date=:d",
                expression_attribute_names={
                    '#status': "STATUS",
                    '#message': "MESSAGE",
                    '#date': "SEND_DATE"
                },
                expression_attribute_values={
                    ':s': str('PENDING'), ':t': json.dumps(api_response),  ':d': send_date})
    
    if db == 'BILLING':
        if update_for == 'success':
            status = db_obj.update_item({"PK": record['PK'], "SK":'METADATA'},
                update_expression = f"set #operations=:r, #status=:s,  #date=:d, #payload_size=:p",
                expression_attribute_names={
                    '#operations': "OPERATIONS",
                    '#status': "STATUS",
                    '#date': "SEND_DATE",
                    '#payload_size': "PAYLOAD_SIZE"
                },
                expression_attribute_values={
                    ':r': json.dumps(record['OPERATIONS']), ':s': "SUBMITTED", ':d': send_date, ':p': record['PAYLOAD_SIZE']})
        
        elif update_for == 'failure':
            status = db_obj.update_item({"PK": record['PK'], "SK":'METADATA'},
                update_expression = f"set #operations=:r, #status=:s,  #date=:d, #payload_size=:p",
                expression_attribute_names={
                    '#operations': "OPERATIONS",
                    '#status': "STATUS",
                    '#date': "SEND_DATE",
                    '#payload_size': "PAYLOAD_SIZE"
                },
                expression_attribute_values={
                    ':r': json.dumps(record['OPERATIONS']), ':s': "FAILED", ':d': send_date, ':p': record['PAYLOAD_SIZE']})
            
        elif update_for == 'pending':
            status = db_obj.update_item({"PK": record['PK'], "SK":'METADATA'},
                update_expression = f"set #operations=:r, #status=:s",
                expression_attribute_names={
                    '#operations': "OPERATIONS",
                    '#status': "STATUS"
                },
                expression_attribute_values={
                    ':r': json.dumps(record['OPERATIONS']), ':s': "PENDING"})
    
    return status
