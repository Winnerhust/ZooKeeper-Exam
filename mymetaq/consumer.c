#include<stdio.h>  
#include<stdlib.h>  
#include<string.h>  
#include<unistd.h>
#include"zookeeper.h"  
#include"zookeeper_log.h"  

char g_host[512]= "172.17.0.36:2181";  
char g_topic[512]= "MyTopic";
char g_my_path[512]={0};
int  g_groupid = 0;
enum MODE{MASTER_MODE,SLAVE_MODE} g_mode;

void print_usage();
void get_option(int argc,const char* argv[]);

/**********unitl*********************/  
void print_usage()
{
    printf("Usage : [consumer] [-h] [-t topic] [-g groudid ] [-s ip:port] \n");
    printf("        -h Show help\n");
    printf("        -t topic name\n");
    printf("        -g groupid\n");
    printf("        -s zookeeper server ip:port\n");
    printf("For example:\n");
    printf("    recive the message from the breaker:\n");
    printf("        >consumer -t MyTopic -g 0 -s 172.17.0.36:2181\n");
}
 
void get_option(int argc,const char* argv[])
{
	extern char    *optarg;
	int            optch;
	int            dem = 1;
	const char    optstring[] = "hg:t:s:";
    
        
	while((optch = getopt(argc , (char * const *)argv , optstring)) != -1 )
	{
		switch( optch )
		{
		case 'h':
			print_usage();
			exit(-1);
		case '?':
			print_usage();
			printf("unknown parameter: %c\n", optopt);
			exit(-1);
		case ':':
			print_usage();
			printf("need parameter: %c\n", optopt);
			exit(-1);
        case 's':
            strncpy(g_host,optarg,sizeof(g_host));
            break;
        case 't':
            strncpy(g_topic,optarg,sizeof(g_topic));
            break;
        case 'g':
            if(optarg){
                g_groupid = atoi(optarg);
            }
            break;
		default:
			break;
		}
	}
}
void convert_str_to_vector(char *str,char * delim,struct String_vector *vector)
{
    int i = 0;

    char *result = strsep( str,delim);
    while( result != NULL ) {
        ++i;
        result = strsep( str,delim );
    }

    if(i > 0){
        vector->count = i;
        vector->data = calloc(sizeof(*(vector->data)),vector->count);
    }else{
        vector->count = 0;
        vector->data = NULL;
    } 

    char *result = strsep( str,delim);
    i = 0;
    while( result != NULL ) {
        vector.data[i++] = strdup(result);
        result = strsep( str,delim );
    }

}
void do_work(zhandle_t *zkhandle)
{
    char group_path[512]={0};
    char breaker_topic_path[512]={0};

    sprintf(group_path,"/Consumer/%s/group-%d",g_topic,g_groupid);
    sprintf(breaker_topic_path,"/Breaker/%s",g_topic);

    if(g_mode == MASTER_MODE){
        set_balancing_strategy(zkhandle,group_path,breaker_topic_path);
    }else{
        //todo
        char str_partitions[512]={0};
        int  len = sizeof(str_partitions);
        int ret = zoo_get(zkhandle,g_my_path,zkget_watcher_g,"",str_partitions,&len,NULL);

        if(ret != ZOK){
            fprintf(stderr,"failed to get data of the path %s.\n",g_my_path);
        }else{
            struct String_vector partitions;
            
            convert_str_to_vector(str_partitions,",",&partitions);
            int i = 0;
            for(i = 0; i < partitions.count; ++i){
                sprintf(msg_path,"/Breaker/%s/Partition-%s",g_topic,partitions.data[i]);
                
            }
        }
        
    }

} 
void zkbalance_watcher_g(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx)  
{  
/*  
    printf("watcher event\n");  
    printf("type: %d\n", type);  
    printf("state: %d\n", state);  
    printf("path: %s\n", path);  
    printf("watcherCtx: %s\n", (char *)watcherCtx);  
*/  
    char group_path[512]={0};
    char breaker_topic_path[512]={0};

    sprintf(group_path,"/Consumer/%s/group-%d",g_topic,g_groupid);
    sprintf(breaker_topic_path,"/Breaker/%s",g_topic);
    
    char *p_self_name = rindex(g_my_path,'/')+1;

    choose_mater(zkhandle,group_path,p_self_name);

    if(g_mode == MASTER_MODE){
        set_balancing_strategy(zh,group_path,breaker_topic_path);
    }else{
    
    }
}  

void choose_mater(zhandle_t *zkhandle,const char *group_path,const char *self_name)
{
    struct String_vector procs;
    int i = 0;
    int ret = zoo_wget_children(zkhandle,group_path,zkbalance_watcher_g,NULL,&procs);
        
    if(ret != ZOK || procs.count == 0){
        fprintf(stderr,"failed to get the children of path %s!\n",group_path);
    }else{
        char master_name[512]={0};
        
        strcpy(master_name,procs.data[0]);

        for(i = 1; i < procs.count; ++i){
            if(strcmp(master_name,procs.data[i])>0){
                strcpy(master_name,procs.data[i]);
            }
        }

        if(strcmp(master_name,self_name) == 0){
            g_mode = MASTER_MODE;
        }else{
            g_mode = SLAVE_MODE;
        }
        
    }

    for(i = 0; i < procs.count; ++i){
        free(procs.data[i]);
        procs.data[i] = NULL;
    }

}
int set_balancing_strategy(zhandle_t *zkhandle,char *group_path,char *breaker_topic_path)
{   
    if(g_mode != MASTER_MODE){
       return 0;
    }
    struct String_vector partitions;
    struct String_vector clients;
    
    int ret = zoo_wget_children(zkhandle,breaker_topic_path,zkbalance_watcher_g,NULL,&partitions);
    if(ret != ZOK){
        fprintf(stderr,"error when get children of path %s.\n",breaker_topic_path); 
        return ret;
    }
    
    ret = zoo_wget_children(zkhandle,group_path,zkbalance_watcher_g,NULL,&clients);
    if(ret != ZOK){
        fprintf(stderr,"error when get children of path %s.\n",group_path); 
        return ret;
    }

    int i = 0;
    char client_path[512]={0};
    char buffer[1024] = {0};

    struct String_vector values;
    if(clients.count){
        values.count = clients.count < partitions.count ? clients.count : partitions.count;

        values.data = calloc(sizeof(*values.data),values.count);
        memset(values.data,0,sizeof(*values.data)*values.count);
    }else{
        values.count = 0;
        values.data = NULL;
    }
    printf("values.count = %d\n",values.count);

    for(i = 0; i < partitions.count; ++i){
        int k = i % clients.count;
        if (values.data[k] == NULL){
            sprintf(buffer,"%d",i); 
            values.data[k] = strdup(buffer);

        }else {
            sprintf(buffer,"%s,%d",values.data[k],i);
            free(values.data[k]);
            values.data[k] = strdup(buffer);
        }
        printf("%d = %s\n",k,values.data[k]);
    }
    for(i = 0; i < values.count; ++i){    
        sprintf(client_path,"/Consumer/%s/group-%d/%s",g_topic,g_groupid,clients.data[i%clients.count]);
        
        ret = zoo_set(zkhandle,client_path,values.data[i],strlen(values.data[i]),-1);
        if(ret != ZOK){
            continue;
        }
        printf("%s = %s\n",client_path,values.data[i]);
        
        free(values.data[i]);
    }
    //多余的consumer将收不到分区消息
    for(i = values.count; i < clients.count; ++i){
        sprintf(client_path,"/Consumer/%s/group-%d/%s",g_topic,g_groupid,clients.data[i%clients.count]);
        
        ret = zoo_set(zkhandle,client_path,"",0,-1);
        if(ret != ZOK){
            continue;
        }
        printf("%s = %s\n",client_path,"");

    }
}

//同步方式创建节点  
void create(zhandle_t *zkhandle,char *path,int flag,const char *ctx)  
{  
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer);  
    

    int ret = zoo_exists(zkhandle,path,0,NULL); 
    if(ret != ZOK){
        ret = zoo_create(zkhandle,path,ctx,strlen(ctx),
                          &ZOO_OPEN_ACL_UNSAFE,flag,
                          path_buffer,bufferlen);
  
        if (ret != ZOK){  
            printf("节点创建失败:%s \n",path);  
            exit(EXIT_FAILURE);  
        }else {  
            printf("创建的节点名称为：%s\n",path_buffer);  
            printf("创建的节点值为：%s\n",(char *)ctx);  
        }
    }else{
        printf("path:%s 已经存在,无需创建\n",path);
    }  
}  


int main(int argc, const char *argv[])  
{  
    int timeout = 30000;  
  
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN); //设置日志级别,避免出现一些其他信息  

    get_option(argc,argv);

    zhandle_t* zkhandle = zookeeper_init(g_host,NULL, timeout, 0, (char *)"Load Balancing Test", 0);  

    if (zkhandle ==NULL){ 
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");  
        exit(EXIT_FAILURE);  
    }  
    
    char consumer_path[512] = "/Consumer";
    char topic_path[512] = {0};
    char group_path[512] = {0};
    char client_path[512] = {0};
    
    sprintf(topic_path,"%s/%s",consumer_path,g_topic);
    sprintf(group_path,"%s/group-%d",topic_path,g_groupid);
    sprintf(client_path,"%s/client-",group_path);
    

    create(zkhandle,consumer_path,0,"");
    create(zkhandle,topic_path,0,"");
    create(zkhandle,group_path,0,"00000000");
    
    int bufferlen = sizeof(g_my_path);

    int ret = zoo_create(zkhandle,client_path,NULL,0,
                          &ZOO_OPEN_ACL_UNSAFE,ZOO_SEQUENCE|ZOO_EPHEMERAL,
                          g_my_path,bufferlen);
  
    if (ret != ZOK){  
        printf("节点创建失败:%s \n",client_path);  
        exit(EXIT_FAILURE);  
    }else {  
        printf("创建的节点名称为：%s\n",g_my_path);  
    }

    char self_name[512] = {0};
    
    strcpy(self_name,rindex(g_my_path,'/')+1);

    
    choose_mater(zkhandle,group_path,self_name);

    do_work(zkhandle);

    getchar();

    zookeeper_close(zkhandle); 

    return 0;
}  
