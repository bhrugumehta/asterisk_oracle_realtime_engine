
/*
* oracle engine for realtime configuration like sip,queue,voicemail
*/

#include <asterisk.h>

#include <asterisk/channel.h>
#include <asterisk/logger.h>
#include <asterisk/config.h>
#include <asterisk/module.h>
#include <asterisk/lock.h>
#include <asterisk/options.h>
#include <asterisk/cli.h>
#include <asterisk/utils.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdio.h>

#include "ocilib.h"

#define ORA_CONFIG "res_oracle.conf"

AST_MUTEX_DEFINE_STATIC(oracle_lock);

static int connected;
static time_t connect_time;

static OCI_Connection *cn;

static char oraconstr[40];/*is it enough? no!*/
static char orauser[15];
static char orapass[15];

static int parse_config(void);
static int closecon(void);
static int oracle_reconnect(void);

static struct ast_variable *realtime_oracle(const char *database, const char *table, va_list ap)
{
	int numFields, i, valsz, j;
	char sql[512], buf[511];
	char tname[25], sfields[20];
	char *stringp, *chunk, *op;
	const char *newparam, *newval;
	struct ast_variable *var=NULL, *prev=NULL;
	
	OCI_Statement  *st;
	OCI_Resultset  *rs;
	OCI_Column *col;
	
	if(!table) {
		ast_log(LOG_WARNING, "ORACLE RealTime: No table specified.\n");
		return NULL;
	}
	
	snprintf(tname, sizeof tname, table);
	
	newparam = va_arg(ap, const char *);
	newval = va_arg(ap, const char *);
	
	if(!newparam || !newval)  {
		ast_log(LOG_WARNING, "ORACLE RealTime: Realtime retrieval requires at least 1 parameter and 1 value to search on.\n");
		return NULL;
	}

	ast_mutex_lock(&oracle_lock);
	if (!oracle_reconnect()) {
		ast_mutex_unlock(&oracle_lock);
		return NULL;
	}

	if(!strchr(newparam, ' ')) op = " ="; else op = "";

	/*if ((valsz = strlen (newval)) * 2 + 1 > sizeof(buf))
		valsz = (sizeof(buf) - 1) / 2;*/
	snprintf(buf, sizeof buf, "%s", newval);

	snprintf(sql, sizeof(sql), "SELECT * FROM %s WHERE %s%s '%s'", table, newparam, op, buf);

	while((newparam = va_arg(ap, const char *))) {
		newval = va_arg(ap, const char *);
		if(!strchr(newparam, ' ')) op = " ="; else op = "";
		/*if ((valsz = strlen (newval)) * 2 + 1 > sizeof(buf))
			valsz = (sizeof(buf) - 1) / 2;*/
		snprintf(buf, sizeof buf, "%s", newval);
		snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql), " AND %s%s '%s'", newparam, op, buf);
	}
	va_end(ap);
	
	ast_log(LOG_DEBUG, "ORACLE RealTime: Retrieve SQL: %s\n", sql);

	st = OCI_StatementCreate(cn);
	OCI_ExecuteStmt(st, sql);
	
	if((rs = OCI_GetResultset(st))) {
		numFields = OCI_GetColumnCount(rs);
		j=1;
		while(OCI_FetchNext(rs)) {
			for(i = 0; i < numFields; i++, j++) {
				col = OCI_GetColumn(rs, j);
				snprintf(sfields, sizeof sfields, "%s", OCI_ColumnGetName(col));
				stringp = OCI_GetString(rs, j);
				while(stringp) {
					chunk = strsep(&stringp, ";");
					if(chunk && !ast_strlen_zero(ast_strip(chunk))) {
						ast_log(LOG_DEBUG, "%s-%s\n", sfields,chunk);
						if(prev) {
							prev->next = ast_variable_new(sfields, chunk);
							if (prev->next) {
								prev = prev->next;
							}
						} else {
							prev = var = ast_variable_new(sfields, chunk);
						}
					}
				}
			}
		}
	} else {                                
		ast_log(LOG_WARNING, "ORACLE RealTime: Could not find any rows in table %s.\n", table);
	}

	ast_mutex_unlock(&oracle_lock);
	OCI_FreeStatement (st);

	return var;
}

static struct ast_config *realtime_multi_oracle(const char *database, const char *table, va_list ap)
{
	int numFields, i, valsz, j;
	char sql[512], buf[511];
	char tname[25], sfields[25];
	const char *initfield = NULL;
	char *stringp, *chunk, *op;
	const char *newparam, *newval;
	
	struct ast_realloca ra;
	struct ast_variable *var=NULL;
	struct ast_config *cfg = NULL;
	struct ast_category *cat = NULL;
		
	OCI_Statement  *st;
	OCI_Resultset  *rs;
	OCI_Column *col;

	snprintf(tname, sizeof tname, table);
	ast_log(LOG_DEBUG, "### Table: %s ###\n", tname);
	
	if(!table) {
		ast_log(LOG_WARNING, "ORACLE RealTime: No table specified.\n");
		return NULL;
	}
	
	memset(&ra, 0, sizeof(ra));

	cfg = ast_config_new();
	
	if (!cfg) {
		ast_log(LOG_WARNING, "Out of memory!\n");
		return NULL;
	}

	newparam = va_arg(ap, const char *);
	newval = va_arg(ap, const char *);
	
	if(!newparam || !newval)  {
		ast_log(LOG_WARNING, "ORACLE RealTime: Realtime retrieval requires at least 1 parameter and 1 value to search on.\n");
		ast_config_destroy(cfg);
		return NULL;
	}

	initfield = ast_strdupa(newparam);
	if(initfield && (op = strchr(initfield, ' '))) {
		*op = '\0';
	}

	ast_mutex_lock(&oracle_lock);
	if (!oracle_reconnect()) {
		ast_mutex_unlock(&oracle_lock);
		return NULL;
	}

	if(!strchr(newparam, ' ')) op = " ="; else op = "";

	/*if ((valsz = strlen (newval)) * 2 + 1 > sizeof(buf))
		valsz = (sizeof(buf) - 1) / 2;*/

	snprintf(buf,sizeof buf,"%s",newval);

	snprintf(sql, sizeof(sql), "SELECT * FROM %s WHERE %s%s '%s'", table, newparam, op, buf);
	while((newparam = va_arg(ap, const char *))) {
		newval = va_arg(ap, const char *);
		if(!strchr(newparam, ' ')) op = " ="; else op = "";
		/*if ((valsz = strlen (newval)) * 2 + 1 > sizeof(buf))
			valsz = (sizeof(buf) - 1) / 2;*/
		snprintf(buf,sizeof buf, "%s", newval);
		snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql), " AND %s%s '%s'", newparam, op, buf);
	}

	if(initfield) {
		snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql), " ORDER BY %s", initfield);
	}

	va_end(ap);

	ast_log(LOG_DEBUG, "ORACLE RealTime: Retrieve SQL: %s\n", sql);

	st = OCI_StatementCreate(cn);
	OCI_ExecuteStmt(st, sql);

	if((rs = OCI_GetResultset(st))) {
		numFields = OCI_GetColumnCount(rs);
		j=1;
	
		while(OCI_FetchNext(rs)) {
			var = NULL;
			cat = ast_category_new("");
			if(!cat) {
				ast_log(LOG_WARNING, "Out of memory!\n");
				continue;
			}
			
			for(i = 0; i < numFields; i++, j++) {
				col = OCI_GetColumn(rs, j);
				snprintf(sfields, sizeof sfields, "%s", OCI_ColumnGetName(col));
				stringp = OCI_GetString(rs, j);
				while(stringp) {
					chunk = strsep(&stringp, ";");
					if(chunk && !ast_strlen_zero(ast_strip(chunk))) {
						if(initfield && !strcasecmp(initfield, sfields)) {
							ast_category_rename(cat, chunk);
						}
						ast_log(LOG_DEBUG, "qm:%s-%s\n", sfields, chunk);
						var = ast_variable_new(sfields, chunk);
						ast_variable_append(cat, var);
					}
				}
			}
			ast_category_append(cfg, cat);
		}
	} else {
		ast_log(LOG_WARNING, "ORACLE RealTime: Could not find any rows in table %s.\n", table);
	}

	ast_mutex_unlock(&oracle_lock);
	OCI_FreeStatement (st);
	
	return cfg;
}

static int update_oracle(const char *database, const char *table, const char *keyfield, const char *lookup, va_list ap)
{
	int numrows=0;/*is it enough?*/
	char sql[512];
	char buf[511]; 
	int valsz;
	const char *newparam, *newval;
	
	OCI_Statement  *st;

	if(!table) {
		ast_log(LOG_WARNING, "ORACLE RealTime: No table specified.\n");
               return -1;
	}
	newparam = va_arg(ap, const char *);
	newval = va_arg(ap, const char *);
	if(!newparam || !newval)  {
		ast_log(LOG_WARNING, "ORACLE RealTime: Realtime retrieval requires at least 1 parameter and 1 value to search on.\n");
               return -1;
	}

	ast_mutex_lock(&oracle_lock);
	if (!oracle_reconnect()) {
		ast_mutex_unlock(&oracle_lock);
		return -1;
	}

	/*if ((valsz = strlen (newval)) * 1 + 1 > sizeof(buf))
		valsz = (sizeof(buf) - 1) / 2;*/

	snprintf(buf, sizeof buf,"%s", newval);
	snprintf(sql, sizeof(sql), "UPDATE %s SET %s = '%s'", table, newparam, buf);

	while((newparam = va_arg(ap, const char *))) {
		newval = va_arg(ap, const char *);
		/*if ((valsz = strlen (newval)) * 2 + 1 > sizeof(buf))
			valsz = (sizeof(buf) - 1) / 2;*/
		snprintf(buf, sizeof buf, "%s", newval);
		snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql), ", %s = '%s'", newparam, buf);
	}
	va_end(ap);
	/*if ((valsz = strlen (lookup)) * 1 + 1 > sizeof(buf))
		valsz = (sizeof(buf) - 1) / 2;*/

	snprintf(buf, sizeof buf, "%s", lookup);
	snprintf(sql + strlen(sql), sizeof(sql) - strlen(sql), " WHERE %s = '%s'", keyfield, buf);

	ast_log(LOG_DEBUG,"ORACLE RealTime: Update SQL: %s\n", sql);
	
	st = OCI_StatementCreate(cn);
	OCI_ExecuteStmt(st, sql);
	OCI_Commit(cn);
	numrows = OCI_GetAffectedRows(st);
	
	ast_mutex_unlock(&oracle_lock);

	ast_log(LOG_DEBUG,"MySQL RealTime: Updated %d rows on table: %s\n", numrows, table);

	if(numrows >= 0)
		return (int)numrows;

	return -1;
}

/*
* Used for reatime static configuration (like normal static .conf file)
* Use ast_config table for all config file like sip.conf,extensions.conf etc. or
* Use different tables for all config file like ast_config_sip for sip.conf, ast_config_ext for extensions.conf etc.
*/
static struct ast_config *config_oracle(const char *database, const char *table, const char *file, struct ast_config *cfg, int withcomments)
{
	OCI_Statement  *st;
	OCI_Resultset  *rs;
	unsigned int num_rows=0;
	struct ast_variable *new_v;
	struct ast_category *cur_cat=NULL;
	char sql[250] = "";
	char last[80] = "";
	int last_cat_metric = 0;

	last[0] = '\0';

	if(!file || !strcmp(file, ORA_CONFIG)) {
		ast_log(LOG_WARNING, "ORACLE RealTime: Cannot configure myself.\n");
		return NULL;
	}
	snprintf(sql, sizeof(sql), "SELECT category, var_name, var_val, cat_metric FROM %s WHERE filename='%s' and commented=0 ORDER BY filename, cat_metric desc, var_metric asc, category, var_name, var_val, id", table, file);

	ast_log(LOG_DEBUG, "ORACLE RealTime: Static SQL: %s\n", sql);

	ast_mutex_lock(&oracle_lock);
	if(!oracle_reconnect()) {
		ast_mutex_unlock(&oracle_lock);
		return NULL;
	}

	st = OCI_StatementCreate(cn);
	OCI_ExecuteStmt(st, sql);

   if((rs = OCI_GetResultset(st))) {
		/*is it really needed, this?*/
		num_rows = OCI_GetAffectedRows(st);
		ast_log(LOG_DEBUG, "ORACLE RealTime: Found %u rows.\n", num_rows);

		while(OCI_FetchNext(rs)) {
			/*load text file, here e.g "#include interal.conf" */
			if(!OCI_IsNull(rs, 2)) { /*ohh, if null then segfualt*/
				if(!strcmp(OCI_GetString(rs, 2), "#include")) {
					if (!ast_config_internal_load(OCI_GetString(rs, 3), cfg, 0)) {
						ast_mutex_unlock(&oracle_lock);
						return NULL;
					}
					continue;
				}
			}
			/*add new category over here*/
			if(strcmp(last, OCI_GetString(rs, 1)) || last_cat_metric != atoi(OCI_GetString(rs, 4))) {
				cur_cat = ast_category_new(OCI_GetString(rs, 1));
				if (!cur_cat) {
					ast_log(LOG_WARNING, "Out of memory!\n");
					break;
				}
				strcpy(last, OCI_GetString(rs, 1));
				last_cat_metric = atoi(OCI_GetString(rs, 4));
				ast_category_append(cfg, cur_cat);
			}
			/*add new variable in category over here*/
			if(!OCI_IsNull(rs, 2) && !OCI_IsNull(rs, 3)) { /*same thing, if null then segfualt*/
				new_v = ast_variable_new(OCI_GetString(rs, 2), OCI_GetString(rs, 3));
				ast_variable_append(cur_cat, new_v);
			}
		}
	} else {
		ast_log(LOG_WARNING, "ORACLE RealTime: Could not find config '%s' in database.\n", file);
	}

	OCI_FreeStatement (st);
	ast_mutex_unlock(&oracle_lock);

	return cfg;
}

static int oracle_reconnect(void)
{
	if((!connected) ) {
		if (!OCI_Initialize(NULL, NULL, OCI_ENV_DEFAULT)) {
  			ast_log(LOG_WARNING, "Oracle RealTime: Insufficient memory to allocate MySQL resource.\n");
			connected = 0;
			return 0;
		}
		cn = OCI_ConnectionCreate(oraconstr, orauser, orapass, OCI_SESSION_DEFAULT);
		if(cn!=NULL) {
			ast_log(LOG_DEBUG, "Oracle RealTime: Successfully connected to database.\n");
			connected = 1;
			connect_time = time(NULL);
			return 1;
		} else {
			ast_log(LOG_ERROR, "Oracle RealTime: Failed to connect database server \n");
			ast_log(LOG_DEBUG, "Oracle RealTime: Cannot Connect\n");
			connected = 0;
			OCI_Cleanup();
			return 0;
		}
	}
	return 1;
}

/*freed!*/
static int closecon(void)
{
	/*Relax!!!*/
   OCI_FreeConnection(cn);
   OCI_Cleanup();
	return 0;
}


static struct ast_config_engine oracle_engine = {
	.name = "oracle",
	.load_func = config_oracle,
	.realtime_func = realtime_oracle,
	.realtime_multi_func = realtime_multi_oracle,
	.update_func = update_oracle
};

static int parse_config (void)
{
	struct ast_config *cfg;
	const char* cstr;
	const char* user;
	const char* pass;

	cfg = ast_config_load(ORA_CONFIG);
	
	if (cfg) {
		if ((cstr = ast_variable_retrieve(cfg, "connection", "connection_string"))) {
        	snprintf(oraconstr, sizeof oraconstr, "%s", cstr);
       	}
		if ((user = ast_variable_retrieve(cfg, "connection", "username"))) {
        	snprintf(orauser, sizeof orauser, "%s", user);
       	}
		if ((pass = ast_variable_retrieve(cfg, "connection", "password"))) {
        	snprintf(orapass, sizeof orapass, "%s", pass);
       	}
		ast_config_destroy(cfg);
	} else {
		ast_log(LOG_ERROR,"Unable to load %s", ORA_CONFIG);
		return 0;
	}

	return 1;
}

static int realtime_oracle_status(int fd, int argc, char **argv)
{
	int ctime = time(NULL) - connect_time;
	char status[256];
	
	if(oracle_reconnect()) {
		ast_log(LOG_NOTICE, "Successfully connected with oracle DB.\n");

		snprintf(status, 255, "Connected to %s", oraconstr);

		if (ctime > 31536000) {
			ast_cli(fd, "%s for %d years, %d days, %d hours, %d minutes, %d seconds.\n", status, ctime / 31536000, (ctime % 31536000) / 86400, (ctime % 86400) / 3600, (ctime % 3600) / 60, ctime % 60);
		} else if (ctime > 86400) {
			ast_cli(fd, "%s for %d days, %d hours, %d minutes, %d seconds.\n", status, ctime / 86400, (ctime % 86400) / 3600, (ctime % 3600) / 60, ctime % 60);
		} else if (ctime > 3600) {
			ast_cli(fd, "%s for %d hours, %d minutes, %d seconds.\n", status, ctime / 3600, (ctime % 3600) / 60, ctime % 60);
		} else if (ctime > 60) {
			ast_cli(fd, "%s for %d minutes, %d seconds.\n", status, ctime / 60, ctime % 60);
		} else {
			ast_cli(fd, "%s for %d seconds.\n", status, ctime);
		}
		return RESULT_SUCCESS;
	} else {
		ast_log(LOG_ERROR, "Error to connect with oracle DB.\n");
		return RESULT_FAILURE;
	}
}

static char cli_realtime_oracle_status_usage[] =
"Usage: realtime oracle status\n"
"       Shows connection information for the ORACLE RealTime driver\n";

static struct ast_cli_entry cli_realtime_oracle_status = {
        { "realtime", "oracle", "status", NULL }, realtime_oracle_status,
        "Shows connection information for the ORACLE RealTime driver", cli_realtime_oracle_status_usage, NULL };

static int load_module(void)
{
	if(!parse_config())
		return 0;

	ast_mutex_lock(&oracle_lock);

	if(!oracle_reconnect()) {
		ast_log(LOG_ERROR, "ORACLE RealTime: Couldn't establish connection.\n");
	}

	ast_config_engine_register(&oracle_engine);
	ast_verbose(VERBOSE_PREFIX_2 "ORACLE RealTime driver loaded.\n");
	ast_cli_register(&cli_realtime_oracle_status);

	ast_mutex_unlock(&oracle_lock);

	return 0;
}

static int unload_module(void)
{
	ast_mutex_lock(&oracle_lock);

	closecon();
	connected = 0;
	ast_cli_unregister(&cli_realtime_oracle_status);
	ast_config_engine_deregister(&oracle_engine);
	ast_verbose(VERBOSE_PREFIX_2 "ORACLE RealTime unloaded.\n");
	ast_module_user_hangup_all();

	ast_mutex_unlock(&oracle_lock);
	return 0;
}

static int reload(void)
{
	ast_mutex_lock(&oracle_lock);

	closecon();
	connected = 0;
	parse_config();

	if(!oracle_reconnect()) {
		ast_log(LOG_ERROR, "ORACLE RealTime: Couldn't establish connection.\n");
	}

	ast_verbose(VERBOSE_PREFIX_2 "ORACLE RealTime reloaded.\n");

	ast_mutex_unlock(&oracle_lock);
	return 0;
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_DEFAULT, "Oracle RealTime Configuration Driver",
		.load = load_module,
		.unload = unload_module,
		.reload = reload,);