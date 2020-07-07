class Udf:
    record_set = set([])
    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    def exists_in_set(record):
        counter = 0
        record_arr = record.split(' ')
        for rec in record_arr:
            if rec in record_set:
                counter = counter + 1
        return counter

    @staticmethod
    def register_udf(spark, record_set_local):
        global record_set
        record_set = record_set_local
        spark.udf.register("exists_in_set", Udf.exists_in_set)


if __name__ == '__main__':
    pass
