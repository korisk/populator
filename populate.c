#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"
/*
 * create table mrow (time bigint, id bigint, value float);
 * insert into mrow values (1,1,1) (3,2,5) (4,5,5);
 * insert into mrow values (1,1,1);
 */


static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

int
main(int argc, char **argv)
{
    const char *conninfo;
    PGconn     *conn;
    PGresult   *res;
    int         nFields;
    int         i,
                j;

    if (argc > 1)
        conninfo = argv[1];
    else
        conninfo = "host=localhost port=5432 user=korisk dbname=test";

    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        exit_nicely(conn);
    }

    /* Start a transaction block */
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);

       /*
 *      * Should PQclear PGresult whenever it is no longer needed to avoid memory
 *      * leaks
 *      */
	
    res = PQprepare(conn, "qu", "insert into mrow values ($1::bigint, $2::bigint, $3::float);",3, NULL);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "Prepare query failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
	
   const char *paramValues[3];
   int         paramLengths[3];
   int         paramFormats[3];	


   long time = 0, _time = 0;
   long id = 0, _id = 0;
  
   union{
	double  f;
	long     i;
   } _value, value;

   _value.f = 1.0;
    value.f = 1.0;
   
   paramValues[0] = (char*)(&time);
   paramLengths[0] = sizeof(time);
   paramFormats[0] = 1;

   paramValues[1] = (char*)(&id);
   paramLengths[1] = sizeof(id);
   paramFormats[1] = 1;
   
   paramValues[2] = (char*)(&value);
   paramLengths[2] = sizeof(value);
   paramFormats[2] = 1;

   for(i=0; i < 3; i++){
   	time = htobe64(_time);
   	id = htobe64(_id);
 	value.i = htobe64(_value.i);

	_time++;
   	_id++;
   	_value.f += 0.1;

	res = PQexecPrepared(conn, "qu", 3, paramValues, paramLengths, paramFormats, 1);
	if (PQresultStatus(res) != PGRES_COMMAND_OK){
		fprintf(stderr, "Exec Prepared query failed: %s", PQerrorMessage(conn));
        	PQclear(res);
	        exit_nicely(conn);
	}
    	PQclear(res);
   }




 /*
     end the transaction */
    res = PQexec(conn, "END");
    PQclear(res);

    /* close the connection to the database and cleanup */
    PQfinish(conn);

    return 0;
}

