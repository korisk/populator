#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"
/*
 * create table triplets (time bigint, id bigint, value float);
 * insert into triplets values (1,1,1) (3,2,5) (4,5,5);
 * insert into triplets values (1,1,1);
 */

#define STEPSIZE	1000
#define STEPS		1000
#define DEVS		10000

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
	
    int populate_constructor(struct popul *this, int size){
	this->vars = size;
	this->mapping = calloc(size, sizeof(int));
	srand(time(NULL));
    }

    int populate_destructor(struct popul *this){
	free(this->mapping);
    }

    int populate(struct popul *this, struct cell * to_fill){
	to_fill->id = (long)((rand()%(this->vars)));
	to_fill->time = this->mapping[to_fill->id]++;
	to_fill->x.f = ((float)rand()/(float)rand());  
	return 0;
    }  


//} class which generates population


struct _cell{
	unsigned int num;
 	union{
		double  _double;
		long    _long;
	};
} __attribute__((packed));

struct triplet{
	unsigned short num;
	struct _cell time;
	struct _cell id;
	struct _cell value;
} __attribute__((packed));



int populate2(struct popul *this, struct triplet * to_fill){
	long id = (long)((rand()%(this->vars)));
	to_fill->id._long = htobe64(id);
	to_fill->time._long = htobe64(this->mapping[id]++);
	double value = ((float)rand()/(float)rand());  
	to_fill->value._long = htobe64(*((long *)(&value))) ;
	return 0;
}  


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

    struct popul class;
    populate_constructor(&class, DEVS);

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

    //init buffer

    struct triplet setit;
    memset((char*)(&setit),0,sizeof(setit));
    setit.num = htons(3); 
    setit.id.num =  htobe32(8);  
    setit.time.num = htobe32(8);  
    setit.value.num = htobe32(8);  
   


 
    res = PQexec(conn, "copy triplets from stdin with binary;");
    if (PQresultStatus(res) != PGRES_COPY_IN)
    {
        fprintf(stderr, "COPY command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    int   stepsize = STEPSIZE;
	
    int bsize = stepsize*sizeof(struct triplet) + 21; //21 - header size 
    int iter = 0;
    unsigned char *buffer =  calloc(bsize,1);
    //signature
    memcpy(buffer, "PGCOPY\n\377\r\n\0",11);
    iter += 11;
    //flags field
    iter += 4;
    //header extension
    iter += 4;
    //tuples
    int ret = PQputCopyData(conn, buffer, iter);
    if(ret == -1)
    {
        fprintf(stderr, "COPY command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    struct triplet *piter = (struct triplet*)(buffer);
		

    //init (set number of fields and its sizes)
    for(i = 0; i < stepsize; i++)
	memcpy(piter++, &setit, sizeof(setit));

    //populate 
    iter = stepsize * sizeof(setit);
    for(j = 0; j<STEPS; j++){  
	piter = (struct triplet*)buffer;
 	for(i = 0; i < stepsize; i++)
		populate2(&class, piter++); 

	ret = PQputCopyData(conn, buffer, iter);
	if(ret == -1)
    	{
        	fprintf(stderr, "COPY command failed: %s", PQerrorMessage(conn));
        	PQclear(res);
        	exit_nicely(conn);
    	}
    }//end population

    //tail
    iter = 0;
    *(buffer + iter++) = 0xff;
    *(buffer + iter++) = 0xff;

    ret = PQputCopyData(conn, buffer, 2);
    if(ret == -1)
    {
        fprintf(stderr, "COPY command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    //send it
    ret = PQputCopyEnd(conn, NULL);
    if(ret == -1)
    {
        fprintf(stderr, "END COPY command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);

 /*
     end the transaction */
    res = PQexec(conn, "END");
    PQclear(res);

    /* close the connection to the database and cleanup */
    PQfinish(conn);
    populate_destructor(&class);

    return 0;
}

