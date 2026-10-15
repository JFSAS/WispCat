//go:build (darwin || linux) && cgo

#include <sqlite3.h>
#include <stddef.h>
#include <string.h>
int rea_keyinfo(const char *path,const char *owner,unsigned char *out,
                size_t capacity,size_t *length,char *digest) {
 sqlite3 *db=NULL;sqlite3_stmt *q=NULL;int rc=-1;
 if(sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
 if(sqlite3_prepare_v2(db,"SELECT key_info_md5,key_info_data FROM LoginKeyInfoTable WHERE user_name_md5=? COLLATE NOCASE ORDER BY rowid DESC LIMIT 1",-1,&q,NULL)!=SQLITE_OK)goto done;
 if(sqlite3_bind_text(q,1,owner,-1,SQLITE_TRANSIENT)!=SQLITE_OK)goto done;
 if(sqlite3_step(q)!=SQLITE_ROW||sqlite3_column_type(q,1)!=SQLITE_BLOB)goto done;
 const unsigned char *hash=sqlite3_column_text(q,0);int n=sqlite3_column_bytes(q,1);
 if(!hash||strlen((const char*)hash)!=32||n<=0||(size_t)n>capacity)goto done;
 memcpy(digest,hash,32);digest[32]=0;memcpy(out,sqlite3_column_blob(q,1),n);*length=(size_t)n;rc=0;
done:
 if(q)sqlite3_finalize(q);if(db)sqlite3_close(db);return rc;
}
