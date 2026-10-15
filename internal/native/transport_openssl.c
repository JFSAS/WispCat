//go:build (darwin || linux) && cgo

#define MMTLS2_SHARED
/* Fresh MMTLS2/TLS1.3 probe, or bounded in-process exchange when built with
 * MMTLS2_SHARED. The probe CLI sends no application data; shared mode uses
 * caller-owned byte buffers and never prints or saves their contents. */
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/rand.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

static unsigned char client_ctx[39];
static int extension_sent, ticket_seen, ticket_tlv_valid;
static size_t ticket_bytes;

static void be32(unsigned char *p, uint32_t value) {
    for (int i=0;i<4;i++) p[i]=(unsigned char)(value>>(24-8*i));
}
static size_t add_field(size_t at, unsigned char tag, const unsigned char *data, size_t n) {
    client_ctx[at++]=tag; client_ctx[at++]=(unsigned char)(n>>8); client_ctx[at++]=(unsigned char)n;
    memcpy(client_ctx+at,data,n); return at+n;
}
static int add_ctx(SSL *s, unsigned int type, unsigned int context, const unsigned char **out,
                   size_t *length, X509 *x, size_t index, int *alert, void *arg) {
    (void)s;(void)type;(void)context;(void)x;(void)index;(void)alert;(void)arg;
    *out=client_ctx; *length=sizeof(client_ctx); extension_sent=1; return 1;
}
static int parse_ticket(SSL *s, unsigned int type, unsigned int context, const unsigned char *in,
                        size_t n, X509 *x, size_t index, int *alert, void *arg) {
    (void)s;(void)type;(void)context;(void)x;(void)index;(void)alert;(void)arg;
    ticket_seen=1;ticket_bytes=n;size_t at=0;
    while (at<n) {
        if (n-at<3) return 0;
        size_t count=((size_t)in[at+1]<<8)|in[at+2];at+=3;
        if (count>n-at) return 0;
        at+=count;
    }
    ticket_tlv_valid=1;return 1;
}
static void quoted(const char *s) {
    putchar('"');
    for (;*s;s++) {
        unsigned char c=(unsigned char)*s;
        if (c=='"'||c=='\\') {putchar('\\');putchar(c);}
        else if (c<32) printf("\\u%04x",c);
        else putchar(c);
    }
    putchar('"');
}
static int dial(const char *ip, const char *port) {
    struct addrinfo hints={0},*addresses=NULL;hints.ai_family=AF_UNSPEC;hints.ai_socktype=SOCK_STREAM;hints.ai_flags=AI_NUMERICHOST;
    if (getaddrinfo(ip,port,&hints,&addresses)) return -1;
    int fd=-1;
    for (struct addrinfo *p=addresses;p;p=p->ai_next) {
        fd=socket(p->ai_family,p->ai_socktype,p->ai_protocol);if(fd<0)continue;
        int flags=fcntl(fd,F_GETFL,0);fcntl(fd,F_SETFL,flags|O_NONBLOCK);
        int result=connect(fd,p->ai_addr,p->ai_addrlen);
        if(result<0 && errno==EINPROGRESS) {
            struct pollfd wait={fd,POLLOUT,0};result=poll(&wait,1,10000);
            if(result>0){int error=0;socklen_t size=sizeof(error);getsockopt(fd,SOL_SOCKET,SO_ERROR,&error,&size);result=error?-1:0;}
            else result=-1;
        }
        if(result==0){fcntl(fd,F_SETFL,flags);struct timeval timeout={10,0};setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));break;}
        close(fd);fd=-1;
    }
    freeaddrinfo(addresses);return fd;
}
#ifndef MMTLS2_SHARED
int main(int argc,char **argv) {
    if(argc!=5){fprintf(stderr,"usage: probe hostname ip port production-roots.pem\n");return 2;}
    unsigned char word[4],random_context[8];size_t at=0;
    if(RAND_bytes(random_context,sizeof(random_context))!=1)return 3;
    const uint32_t values[]={1,0x8137ce63,0,9};
    for(int i=0;i<4;i++){be32(word,values[i]);at=add_field(at,(unsigned char)(i+1),word,4);}
    at=add_field(at,5,random_context,8);if(at!=sizeof(client_ctx))return 4;
    SSL_CTX *ctx=SSL_CTX_new(TLS_client_method());if(!ctx)return 5;
    SSL_CTX_set_min_proto_version(ctx,TLS1_3_VERSION);SSL_CTX_set_max_proto_version(ctx,TLS1_3_VERSION);
    SSL_CTX_set_verify(ctx,SSL_VERIFY_PEER,NULL);
    int configured=SSL_CTX_load_verify_locations(ctx,argv[4],NULL)==1
        && SSL_CTX_set_ciphersuites(ctx,"TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384")==1
        && SSL_CTX_set1_groups_list(ctx,"X25519:P-256")==1
        && SSL_CTX_add_custom_ext(ctx,61937,SSL_EXT_CLIENT_HELLO,add_ctx,NULL,NULL,NULL,NULL)==1
        && SSL_CTX_add_custom_ext(ctx,61938,SSL_EXT_TLS1_3_NEW_SESSION_TICKET,NULL,NULL,NULL,parse_ticket,NULL)==1;
    SSL *ssl=configured?SSL_new(ctx):NULL;int fd=ssl?dial(argv[2],argv[3]):-1;
    int result=-1,ssl_error=0;long verify=-1;char error[256]="";
    if(ssl && fd>=0 && SSL_set_tlsext_host_name(ssl,argv[1])==1 && SSL_set1_host(ssl,argv[1])==1 && SSL_set_fd(ssl,fd)==1) {
        result=SSL_connect(ssl);ssl_error=result==1?0:SSL_get_error(ssl,result);verify=SSL_get_verify_result(ssl);
        if(result==1){fcntl(fd,F_SETFL,fcntl(fd,F_GETFL,0)|O_NONBLOCK);unsigned char byte;int peek=SSL_peek(ssl,&byte,1);(void)peek;}
    }
    unsigned long code=ERR_peek_last_error();if(code)ERR_error_string_n(code,error,sizeof(error));
    printf("{\"hostname\":");quoted(argv[1]);printf(",\"connect_ip\":");quoted(argv[2]);
    printf(",\"configured\":%s,\"tcp_connected\":%s,\"handshake_success\":%s,\"ssl_error\":%d,\"verify_result\":%ld,\"error\":",configured?"true":"false",fd>=0?"true":"false",result==1?"true":"false",ssl_error,verify);quoted(error);
    printf(",\"tls_version\":");quoted(result==1?SSL_get_version(ssl):"");printf(",\"cipher\":");quoted(result==1?SSL_get_cipher_name(ssl):"");
    printf(",\"client_extension_sent\":%s,\"client_extension_bytes\":39,\"ticket_extension_seen\":%s,\"ticket_bytes\":%zu,\"ticket_tlv_valid\":%s,\"account_credentials_used\":false,\"application_data_sent\":false,\"account_authentication_tested\":false}\n",extension_sent?"true":"false",ticket_seen?"true":"false",ticket_bytes,ticket_tlv_valid?"true":"false");
    if(ssl)SSL_free(ssl);if(fd>=0)close(fd);SSL_CTX_free(ctx);return result==1?0:1;
}
#else
/* One serial exchange. Request/response buffers remain in the caller process;
 * no raw values are printed or saved. This ABI intentionally has no CLI mode.
 */
struct exchange_metadata {
    int configured, tcp_connected, handshake_success, ssl_error;
    long verify_result;
    size_t written, received;
    int extension_sent, read_finished, read_error;
    char tls_version[32], cipher[96];
};
int mmtls2_exchange(const char *hostname,const char *ip,const char *port,const char *roots,
                   const unsigned char *request,size_t request_size,
                   unsigned char *response,size_t response_capacity,
                   struct exchange_metadata *metadata) {
    if(!hostname||!ip||!port||!roots||!request||!request_size||!response||!metadata
        ||request_size>1048576||response_capacity>4194304)return -2;
    memset(metadata,0,sizeof(*metadata));metadata->verify_result=-1;
    extension_sent=ticket_seen=ticket_tlv_valid=0;ticket_bytes=0;
    unsigned char word[4],random_context[8];size_t at=0;
    if(RAND_bytes(random_context,sizeof(random_context))!=1)return -3;
    const uint32_t values[]={1,0x8137ce63,0,9};
    for(int i=0;i<4;i++){be32(word,values[i]);at=add_field(at,(unsigned char)(i+1),word,4);}
    at=add_field(at,5,random_context,8);if(at!=sizeof(client_ctx))return -4;
    SSL_CTX *ctx=SSL_CTX_new(TLS_client_method());if(!ctx)return -5;
    SSL_CTX_set_min_proto_version(ctx,TLS1_3_VERSION);SSL_CTX_set_max_proto_version(ctx,TLS1_3_VERSION);
    SSL_CTX_set_verify(ctx,SSL_VERIFY_PEER,NULL);
    metadata->configured=SSL_CTX_load_verify_locations(ctx,roots,NULL)==1
        && SSL_CTX_set_ciphersuites(ctx,"TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384")==1
        && SSL_CTX_set1_groups_list(ctx,"X25519:P-256")==1
        && SSL_CTX_add_custom_ext(ctx,61937,SSL_EXT_CLIENT_HELLO,add_ctx,NULL,NULL,NULL,NULL)==1
        && SSL_CTX_add_custom_ext(ctx,61938,SSL_EXT_TLS1_3_NEW_SESSION_TICKET,NULL,NULL,NULL,parse_ticket,NULL)==1;
    SSL *ssl=metadata->configured?SSL_new(ctx):NULL;
    int fd=ssl?dial(ip,port):-1;metadata->tcp_connected=fd>=0;int status=-1;
    if(ssl&&fd>=0&&SSL_set_tlsext_host_name(ssl,hostname)==1&&SSL_set1_host(ssl,hostname)==1&&SSL_set_fd(ssl,fd)==1) {
        int result=SSL_connect(ssl);
        metadata->ssl_error=result==1?0:SSL_get_error(ssl,result);
        metadata->verify_result=SSL_get_verify_result(ssl);
        metadata->handshake_success=result==1;
        metadata->extension_sent=extension_sent;
        if(result==1) {
            snprintf(metadata->tls_version,sizeof(metadata->tls_version),"%s",SSL_get_version(ssl));
            snprintf(metadata->cipher,sizeof(metadata->cipher),"%s",SSL_get_cipher_name(ssl));
            status=0;
            while(metadata->written<request_size) {
                size_t count=0;
                if(SSL_write_ex(ssl,request+metadata->written,request_size-metadata->written,&count)!=1||!count){status=-6;break;}
                metadata->written+=count;
            }
            if(status==0) {
                while(metadata->received<response_capacity) {
                    size_t count=0;
                    int ok=SSL_read_ex(ssl,response+metadata->received,response_capacity-metadata->received,&count);
                    metadata->received+=count;
                    if(ok!=1) {
                        int error=SSL_get_error(ssl,ok);
                        metadata->read_finished=error==SSL_ERROR_ZERO_RETURN;
                        metadata->read_error=metadata->read_finished?0:error;
                        break;
                    }
                    if(!count){metadata->read_error=-1;break;}
                }
                if(metadata->received==response_capacity)status=-7;
            }
        }
    }
    if(ssl)SSL_free(ssl);if(fd>=0)close(fd);SSL_CTX_free(ctx);return status;
}
#endif
