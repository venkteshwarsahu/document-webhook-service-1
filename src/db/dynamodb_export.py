"""
Export DynamoDb Module
"""
import json
import csv
from io import StringIO, BytesIO
import boto3
from pprint import pprint
from boto3.dynamodb.conditions import Key


def exportFromDynamoDB(
    table,
    gsipk,
    gsipk_value,
    gsisk,
    gsisk_value,
    gsi="inno_document_dev_registry_gsi1",
    output="batch_processed_data.csv",
    format="csv",
):
    """
        Export DynamoDb Table.

        Args:
            table (String): Table name want to query
            format (String) [OPTIONAL]: csv or json
            output (String): Name of output filename
            columns (String): Array of columns needed in output
            col_val (String): Query against specific field,
                            <column_name>=<value1>,<value2>,...

                            ex. batch_name="2213312","3345123",...

        Returns:
            Object (Obj): csv dataframe object

    """

    print("export dynamodb: {}".format(table))
    data = read_dynamodb_data(
        table,
        gsi=gsi,
        gsipk=gsipk,
        gsipk_value=gsipk_value,
        gsisk=gsisk,
        gsisk_value=gsisk_value,
    )
    if format != "csv":
        output_filename = table + ".json"
        if output is not None:
            output_filename = output
        write_to_json_file(data, output_filename)
    else:
        output_filename = table + ".csv"
        if output is not None:
            output_filename = output
        csv_str = write_to_csv_file(data, output_filename)

        return csv_str, data["items"]


def get_keys(data):
    keys = set([])
    for item in data:
        keys = keys.union(set(item.keys()))
    return keys


def read_dynamodb_data(table, gsi, gsipk, gsipk_value, gsisk, gsisk_value):
    """
    Query batch-GSI item from dynamodb.
    :param table: String
    :return: Data in Dictionary Format.
    """

    my_session = boto3.session.Session()
    my_region = my_session.region_name

    print(f"Connecting to AWS DynamoDb, region: {my_region}")
    dynamodb_resource = boto3.resource("dynamodb", region_name="ap-south-1")
    table = dynamodb_resource.Table(table)

    print("Downloading ", end="")
    keys = []
    for item in table.attribute_definitions:
        keys.append(item["AttributeName"])
    keys_set = set(keys)
    item_count = table.item_count

    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"GlobalSecondaryIndex: {gsi}")
    print(f"Key PK: {gsipk}")
    print(f"Vaue PK Value: {gsipk_value}")
    print(f"Key SK: {gsisk}")
    print(f"Vaue SK Value: {gsisk_value}")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    raw_data = table.query(
        IndexName=gsi,
        KeyConditionExpression=Key(gsipk).eq(gsipk_value) & Key(gsisk).eq(gsisk_value),
    )
    if raw_data is None:
        return None

    items = raw_data["Items"]
    fieldnames = set([]).union(get_keys(items))
    cur_total = len(items) + raw_data["Count"]
    if cur_total > item_count:
        percents = 99.99
    else:
        if item_count != 0:
            percents = cur_total * 100 / item_count
        else:
            percents = 0

    print("{} records ..... {:02.0f}%".format(raw_data["Count"], percents), end="\r")
    while raw_data.get("LastEvaluatedKey"):
        print(f"Running query from LastEvaluatedKey: {raw_data['LastEvaluatedKey']}")
        print("Downloading ", end="")
        raw_data = table.query(
            IndexName=gsi,
            KeyConditionExpression=Key(gsipk).eq(gsipk_value)
            & Key(gsisk).eq(gsisk_value),
            ExclusiveStartKey=raw_data["LastEvaluatedKey"],
        )
        items.extend(raw_data["Items"])
        fieldnames = fieldnames.union(get_keys(items))
        cur_total = len(items) + raw_data["Count"]
        if cur_total > item_count:
            percents = 99.99
        else:
            if item_count != 0:
                percents = cur_total * 100 / item_count
            else:
                percents = 0

        print(
            "{} records ..... {:02.0f}%".format(raw_data["Count"], percents), end="\r"
        )
    print()
    print("Total downloaded records: {}".format(len(items)))

    for fieldname in fieldnames:
        if fieldname not in keys_set:
            keys.append(fieldname)
    return {"items": items, "keys": keys}


def convert_rawdata_to_stringvalue(data):
    """
    Convert raw data to string value.
    :param data: List of dictionary
    :return: String value.
    """
    ret = []
    for item in data:
        obj = {}
        for k, v in item.items():
            obj[k] = str(v)
        ret.append(obj)
    return ret


def write_to_json_file(data, filename):
    """
    Write to a json file
    :param data: Dictionary
    :param filename: output file name.
    :return: None
    """
    if data is None:
        return

    print("Writing to json file.")
    with open(filename, "w") as f:
        f.write(json.dumps(convert_rawdata_to_stringvalue(data["items"])))


def getFilteredData(raw_data, col_val_array):
    filter_data = []
    for rd in raw_data:
        if rd[col_val_array[0]] in col_val_array[1]:
            filter_data.append(rd)

    return filter_data


def write_to_csv_file(data, filename):
    """
    Write to a csv file.
    :param data:
    :param filename:
    :return:
    """
    if data is None:
        return

    print("Writing to csv file.")

    print(data)

    # csvfile = StringIO()

    keys = ["masking", "NameMatch", "GenderMatch", "DOBMatch", "AddressMatch", "facematch"]

    csv_string = "RequestId,MaskingStatus,NameMatch,GenderMatch,DOBMatch,AddressMatch,FaceMatch,Status\n"

    for d in data["items"]:
        print(d)
        parsed_json = json.loads(d['OPERATIONS'])

        for k in keys:
            if k not in parsed_json:
                parsed_json[k] = 'false'
            else:
                parsed_json[k] = 'true'

        s = ""
        s += d['PK'].split("REQUESTID#")[-1] + ','
        s += parsed_json['masking'] + ','
        s += parsed_json['NameMatch'] + ','
        s += parsed_json['GenderMatch'] + ','
        s += parsed_json['DOBMatch'] + ','
        s += parsed_json['AddressMatch'] + ','
        s += parsed_json['facematch'] + ','
        s += d['STATUS'] + '\n'

        csv_string += s

    print(csv_string)
    # pprint(data)
    # custom_data = []
    # if col_array is not None:
    #     data["keys"] = col_array
    #     for itm in data["items"]:
    #         res = dict((k, itm[k]) for k in col_array if k in itm)
    #         custom_data.append(res)
    # writer = csv.DictWriter(
    #     csvfile, delimiter=",", fieldnames=data["keys"], quotechar='"'
    # )
    # writer.writeheader()
    # if custom_data != []:
    #     # pprint(custom_data)
    #     if col_val_array is not None:
    #         custom_data = getFilteredData(custom_data, col_val_array)
    #         if len(custom_data) == 0:
    #             print("Given Value not found...")
    #         writer.writerows(custom_data)
    #     else:
    #         writer.writerows(custom_data)

    # else:
    #     # pprint(data["items"])
    #     if col_val_array is not None:
    #         filter_data = getFilteredData(data["items"], col_val_array)
    #         if len(filter_data) == 0:
    #             print("Given Value not found...")
    #         writer.writerows(filter_data)
    #     else:
    #         writer.writerows(data["items"])

    return csv_string
