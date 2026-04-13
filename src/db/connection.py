from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
from ..utils.exceptions import InvalidDateException

class DBClient:

    # create dynamodb boto client
    dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")

    # init method
    def __init__(self, table: str, gsi: str) -> None:
        # store table in self object
        self.table: DBClient.dynamodb.Table = DBClient.dynamodb.Table(table)
        self.gsi = gsi

    def put_item(self, data: object):
        """
        Method to insert data in db
        Attributes:
            data -- data to insert
                Sample / Imp keys --
                    {
                        PK: SRID#<srid>,
                        SK: METADATA
                        GSI-PK: DATE IN FORMAT (YYYYMM)
                        GSI-SK: DATE (DD)
                        STATUS: true/false (string)
                        ... any other keys required
                    }
        """

        resp = self.table.put_item(Item=data)
        return resp

    def update_item(
        self,
        key: object,
        update_expression: str,
        expression_attribute_names:object,
        expression_attribute_values: object = {},
        return_value: str = "UPDATED_NEW",
    ):
        """
        Method to insert data in db
        Doc Link: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
        Attributes:
            data -- data to insert
                Sample / Imp keys --
                    {
                        PK: SRID#<srid>,
                        SK: METADATA
                        GSI-PK: DATE IN FORMAT (YYYYMM)
                        GSI-SK: DATE (DD)
                        STATUS: true/false (string)
                        ... any other keys required
                    }
        """

        resp = self.table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues=return_value,
        )
        return resp

    def query(self, pk: str, sk: str):
        """
        Execute raw db query
        Attributes:
            pk -- partition key
            sk -- sorting key
        """

        q = {"PK": pk, "SK": sk}
        resp = self.table.get_table(Key=q)
        return resp

    def query_gsi(self, pk: str, sk: str, status: list):
        """
        Execute raw db query
        Attributes:
            pk -- partition key
            sk -- sorting key
        """

        items = []
        resp = self.table.query(
            IndexName=self.gsi,
            KeyConditionExpression=Key("GSI-PK").eq(pk) & Key("GSI-SK").eq(sk),
            FilterExpression=Attr("STATUS").eq(status[0])
        )

        items.extend(resp['Items'])
    
        while resp.get("LastEvaluatedKey"):
            print(f"Running query from LastEvaluatedKey: {resp['LastEvaluatedKey']}")
            print("Downloading ", end="")
            resp = self.table.query(
                IndexName=self.gsi,
                KeyConditionExpression=Key("GSI-PK").eq(pk) & Key("GSI-SK").eq(sk),
                FilterExpression=Attr("STATUS").eq(status[0]),
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            print(resp);
            items.extend(resp["Items"])
            
        return {'Items': items}

    def get_data_by_date(self, date: str, status: list):
        """
        Method to get data by date from db
        Attributes:
            date -- data input date in the format DD-MM-YYYY
        """

        splitted_date = date.split("-")

        date = splitted_date[0]
        month = splitted_date[1]
        year = splitted_date[2]

        if len(splitted_date) < 3:
            raise InvalidDateException(date)

        # partition key of db
        pk = splitted_date[2] + splitted_date[1]
        sk = splitted_date[0]

        resp = self.query_gsi(pk, sk, status)
        return resp["Items"] if "Items" in resp.keys() else [], date, month, year

    def get_todays_data(self, status: list = ["true", "false"]):
        """
        Method to get todays data based on system time
        Attributes:
            status -- list of string containing status value we need
        """

        ts = datetime.now()

        date = ts.day
        month = ts.month
        year = ts.year

        date = f"0{date}" if date < 10 else date
        month = f"0{month}" if month < 10 else month

        resp = self.get_data_by_date(f"{date}-{month}-{year}", status)
        return resp, date, month, year
        return resp["Items"] if "Items" in resp.keys() else []
