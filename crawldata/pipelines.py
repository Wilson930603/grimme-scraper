#pip install mysql-connector-python
import mysql.connector,json
from mysql.connector import Error
from crawldata.functions import *
from datetime import datetime
class CrawldataPipeline:
    def open_spider(self,spider):
        self.DATABASE_NAME='PARTAO_Product_Data'
        self.HOST='partaodb.cpy6cs0k6e9n.eu-central-1.rds.amazonaws.com'
        self.username='admin'
        self.password='dC3EiEq9QO3G65Ztm3XPsIS3bRb0m5n1Yx295t3Qb'
        try:
            self.conn = mysql.connector.connect(host=self.HOST,database=self.DATABASE_NAME,user=self.username,password=self.password,charset='utf8')
            if self.conn.is_connected():
                print('Connected to DB')
                db_Info = self.conn.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
            else:
                print('Not connect to DB')
        except Error as e:
            print("Error while connecting to MySQL", e)
            self.conn=None
    def close_spider(self,spider):
        if self.conn.is_connected():
            self.conn.close()
    def process_item(self, item, spider):
        # 5_grimme_cat (insert only)
        sql="SELECT * FROM `5_grimme_cat` WHERE cat_id='"+item['cat_id']+"'"
        CHK=get_data_db(self.conn,sql)
        if len(CHK)==0:
            FIELDS=['sku','category','subcategory','brand','cat_id']
            VALUES=[]
            for k in FIELDS:
                VALUES.append("'"+str(item[k]).replace("'","\'")+"'")
            sql="INSERT INTO `5_grimme_cat` ("+(",".join(FIELDS))+") VALUES("+(",".join(VALUES))+")"
            RUNSQL(self.conn,sql)
        # 5_grimme
        sql="SELECT * FROM `5_grimme` WHERE cat_id='"+item['cat_id']+"' AND id='"+item['id']+"'"
        CHK=get_data_db(self.conn,sql)
        if len(CHK)==0:
            FIELDS=['id','cat_id','brand','sku','name','price','part_number','description','image','fitment_data','equipment_fit','url','additional_images','dimensions','weight']
            VALUES=[]
            for k in FIELDS:
                if item[k] is None:
                    item[k]=''
                if k=='price':
                    VALUES.append(str(item[k]).replace("'","\'"))
                elif k in ('additional_images','dimensions','weight'):
                    VALUES.append("'"+json.dumps(item[k]).replace("'","\'")+"'")
                else:
                    VALUES.append("'"+str(item[k]).replace("'","\'")+"'")
            sql="INSERT INTO `5_grimme` (created_date,"+(",".join(FIELDS))+") VALUES('"+spider.DATE_CRAWL+"',"+(",".join(VALUES))+")"
            RUNSQL(self.conn,sql)
        else:
            FIELDS=['brand','sku','name','price','part_number','description','image','fitment_data','equipment_fit','url','additional_images','dimensions','weight']
            UPDATE=[]
            for k in FIELDS:
                if item[k] is None:
                    item[k]=''
                if k=='price':
                    UPDATE.append(k+"="+str(item[k]).replace("'","\'"))
                elif k in ('additional_images','dimensions','weight'):
                    UPDATE.append(k+"='"+json.dumps(item[k]).replace("'","\'")+"'")
                else:
                    UPDATE.append(k+"='"+str(item[k]).replace("'","\'")+"'")
            sql="UPDATE `5_grimme` SET updated_at='"+spider.DATE_CRAWL+"',"+(",".join(UPDATE))+" WHERE cat_id='"+item['cat_id']+"' AND id='"+item['id']+"'"
            RUNSQL(self.conn,sql)
            pass