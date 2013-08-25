#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"
/*
 * create table triplets (time bigint, id bigint, value float);
 * insert into triplets values (1,1,1) (3,2,5) (4,5,5);
 * insert into triplets values (1,1,1);
 */


   struct cell{
	long time;
	long id; 
 	union{
		double  f;
		long     i;
	} x;
   };

//{ class which generates population
    struct popul{
	int *mapping;
	int vars;
    };	
    int populate_constructor(popul *this, int size){
	this->mapping = calloc(size, sizeof(int));
    }

    int populate_destructor(pupul *this){
	free(mapping);
    }

    int pupulate(popul *this, struct cell * to_fill){
	return 0;
    }  


//} class which generates population









#define TRIPLETS	300

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
	
    char *query = malloc(100000);

    int size = sprintf(query, "insert into triplets values ");
    for(i=0;i < TRIPLETS-1; i++){
	size += sprintf(query + size, "($%d::bigint, $%d::bigint, $%d::float),", 
			i*(3) + 1, i*(3) + 2,i*(3) + 3);
    }	
    size += sprintf(query + size, "($%d::bigint, $%d::bigint, $%d::float);", 
			i*(3) + 1, i*(3) + 2,i*(3) + 3);
  
    //fprintf(stderr,"%s\n", query);
    res = PQprepare(conn, "qu", query, TRIPLETS*3, NULL);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "Prepare query failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    free(query);
	
   const char **paramValues = malloc(TRIPLETS*3 * sizeof(const char*));
   int         *paramLengths = malloc(TRIPLETS*3 * sizeof(int));
   int         *paramFormats = malloc(TRIPLETS*3 * sizeof(int));	





	struct   cell * c = malloc(sizeof(struct cell)*TRIPLETS);
  	struct cell *_c = malloc(sizeof(struct cell)*TRIPLETS);


   for(j=0;j < TRIPLETS;j++){	
	_c[j].time = 0;
	_c[j].id = 0;
	_c[j].x.f = 0.1;
   }
 
   for(i=0; i < TRIPLETS;i++){

 	paramValues[i*(3)] = (char*)(&(c[i].time));
   	paramLengths[i*(3)] = sizeof(c[i].time);
   	paramFormats[i*(3)] = 1;

	paramValues[i*(3) + 1] = (char*)(&(c[i].id));
	paramLengths[i*(3) + 1] = sizeof(c[i].id);
	paramFormats[i*(3) + 1] = 1;
   
	paramValues[i*(3) + 2] = (char*)(&(c[i].x));
	paramLengths[i*(3) + 2] = sizeof(c[i].x);
	paramFormats[i*(3) + 2] = 1;
   }

   for(i=0; i < 1000; i++){
	for(j=0;j < TRIPLETS;j++){	

	   	c[j].time = htobe64(_c[j].time);
   		c[j].id = htobe64(_c[j].id);
 		c[j].x.i = htobe64(_c[j].x.i);

		_c[j].time++;
   		_c[j].id++;
   		_c[j].x.f += 0.1;
	}

	res = PQexecPrepared(conn, "qu", (TRIPLETS*3), paramValues, paramLengths, paramFormats, 1);
	if (PQresultStatus(res) != PGRES_COMMAND_OK){
		fprintf(stderr, "Exec Prepared query failed: %s", PQerrorMessage(conn));
        	PQclear(res);
	        exit_nicely(conn);
	}
    	PQclear(res);
   }

   free(c);
   free(_c);
   free(paramValues);
   free(paramLengths);
   free(paramFormats);

 /*
     end the transaction */
    res = PQexec(conn, "END");
    PQclear(res);

    /* close the connection to the database and cleanup */
    PQfinish(conn);

    return 0;
}

