import psycopg
import io

class L:
    def __init__(self, db_config: dict, schema='raw', table='usgs_raw'):
        self.db_config = db_config
        self.schema = schema
        self.table = table

    def connect(self):
        return psycopg.connect(
            host=self.db_config["host"],
            dbname=self.db_config["dbname"],
            user=self.db_config["user"],
            password=self.db_config["password"],
            port=self.db_config.get("port", 5432),
            autocommit=True
        )

    def create_table_if_not_exists(self):
        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};

        CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
            id TEXT PRIMARY KEY,
            mag DOUBLE PRECISION,
            mag_type TEXT,
            place TEXT,
            time BIGINT,
            updated BIGINT,
            tz INT,
            url TEXT,
            detail TEXT,
            felt INT,
            cdi DOUBLE PRECISION,
            mmi DOUBLE PRECISION,
            alert TEXT,
            status TEXT,
            tsunami INT,
            sig INT,
            net TEXT,
            code TEXT,
            ids TEXT,
            sources TEXT,
            types TEXT,
            nst INT,
            dmin DOUBLE PRECISION,
            rms DOUBLE PRECISION,
            gap DOUBLE PRECISION,
            eq_type TEXT,
            title TEXT,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            depth DOUBLE PRECISION,
            ingestion_time TIMESTAMP DEFAULT now()
        );
        """
        with self.connect() as conn:
            conn.execute(ddl)
        print(f"Table {self.schema}.{self.table} ensured.")

    def load_raw(self, data: list):
        if not data:
            print("No data to load.")
            return

        self.create_table_if_not_exists()

        # Prepare CSV buffer for COPY
        buffer = io.StringIO()

        for rec in data:
            props = rec.get("properties", {})
            geom = rec.get("geometry", {})
            coords = geom.get("coordinates", [None, None, None])

            row = [
                rec.get("id"),
                props.get("mag"),
                props.get("magType"),
                props.get("place"),
                props.get("time"),
                props.get("updated"),
                props.get("tz"),
                props.get("url"),
                props.get("detail"),
                props.get("felt"),
                props.get("cdi"),
                props.get("mmi"),
                props.get("alert"),
                props.get("status"),
                props.get("tsunami"),
                props.get("sig"),
                props.get("net"),
                props.get("code"),
                props.get("ids"),
                props.get("sources"),
                props.get("types"),
                props.get("nst"),
                props.get("dmin"),
                props.get("rms"),
                props.get("gap"),
                props.get("type"),
                props.get("title"),
                coords[0],
                coords[1],
                coords[2],
            ]

            buffer.write("\t".join("\\N" if v is None else str(v) for v in row) + "\n")


        buffer.seek(0)




        with self.connect() as conn:
            buffer.seek(0)
            copy_sql = f"""
                COPY {self.schema}.{self.table} (
                    id, mag, mag_type, place, time, updated, tz, url, detail, felt,
                    cdi, mmi, alert, status, tsunami, sig, net, code, ids, sources,
                    types, nst, dmin, rms, gap, eq_type, title,
                    longitude, latitude, depth
                ) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')
            """
    
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    for line in buffer:
                        copy.write(line)
    
            conn.commit()  # Explicit commit
            print(f"COPY succeeded. Verify: SELECT count(*) FROM {self.schema}.{self.table};")



