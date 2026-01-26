// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

#define _GNU_SOURCE
#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <mosquitto.h>

#define MAX_AIRPORTS 64
#define MAX_ICAO 8
#define MAX_COUNTRY 8
#define MAX_IATA 8
#define MAX_URL 256
#define MAX_TOPIC 256
#define MAX_NAME 128

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

typedef struct station {
    char icao[MAX_ICAO];
    char name[MAX_NAME];
    char country[MAX_COUNTRY];
    char iata[MAX_IATA];
    double lat, lon, elev;
    struct station *next;
} station_t;

typedef struct {
    char icao[MAX_ICAO];
    int fetch_metar, fetch_taf, interval;
    time_t last_fetch;
} airport_t;

typedef struct {
    char broker[256];
    char client_id[64];
    char topic_prefix[128];
    char username[64];
    char password[64];
    char stations_file[256];
    int default_metar, default_taf, default_interval;
    airport_t airports[MAX_AIRPORTS];
    int airport_count;
} config_t;

typedef struct {
    char *data;
    size_t size;
} buffer_t;

#define HASH_SIZE 1024
static station_t *station_hash[HASH_SIZE];
static volatile int running = 1;
static struct mosquitto *mosq = NULL;
static config_t cfg;

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static const char *parse_skip_ws(const char *p) {
    while (*p && isspace(*p))
        p++;
    return p;
}
static const char *parse_quoted(const char *p, char *out, size_t outsz) {
    char quote = *p;
    if (quote != '\'' && quote != '"')
        return NULL;
    p++;
    size_t i = 0;
    while (*p && *p != quote && i < outsz - 1)
        out[i++] = *p++;
    out[i] = 0;
    if (*p == quote)
        p++;
    return p;
}
static const char *parse_value_str(const char *p, char *out, size_t outsz) {
    p = parse_skip_ws(p);
    if (*p == '\'' || *p == '"')
        return parse_quoted(p, out, outsz);
    size_t i = 0;
    while (*p && *p != ',' && *p != '}' && !isspace(*p) && i < outsz - 1)
        out[i++] = *p++;
    out[i] = 0;
    return p;
}
static const char *parse_value_num(const char *p, double *out) {
    p = parse_skip_ws(p);
    char *end;
    *out = strtod(p, &end);
    return end;
}

static unsigned hash_icao(const char *icao) {
    unsigned h = 0;
    while (*icao)
        h = h * 31 + (unsigned char)toupper(*icao++);
    return h % HASH_SIZE;
}

static void station_add(station_t *st) {
    unsigned h = hash_icao(st->icao);
    st->next = station_hash[h];
    station_hash[h] = st;
}

static int stations_load(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "stations: cannot open file: %s\n", path);
        return -1;
    }
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data = malloc(len + 1);
    fread(data, 1, len, f);
    data[len] = 0;
    fclose(f);

    int count = 0;
    const char *p = data;

    while (*p && *p != '{')
        p++;
    if (*p == '{')
        p++;

    while (*p) {
        p = parse_skip_ws(p);
        if (*p == '}')
            break;
        if (*p == ',') {
            p++;
            continue;
        }

        char icao[MAX_ICAO] = {0};
        if (*p == '\'' || *p == '"')
            p = parse_quoted(p, icao, sizeof(icao));
        else if (isalnum(*p)) {
            size_t i = 0;
            while (*p && (isalnum(*p) || *p == '_') && i < MAX_ICAO - 1)
                icao[i++] = *p++;
            icao[i] = 0;
        } else {
            p++;
            continue;
        }
        if (!icao[0])
            continue;

        p = parse_skip_ws(p);
        if (*p == ':')
            p++;
        p = parse_skip_ws(p);
        if (*p != '{')
            continue;
        p++;

        station_t *st = calloc(1, sizeof(station_t));
        for (char *c = icao; *c; c++)
            *c = toupper(*c);
        memcpy(st->icao, icao, sizeof(st->icao));

        while (*p && *p != '}') {
            p = parse_skip_ws(p);
            if (*p == '}')
                break;

            char key[32] = {0};
            if (*p == '\'' || *p == '"')
                p = parse_quoted(p, key, sizeof(key));
            else {
                size_t i = 0;
                while (*p && *p != ':' && !isspace(*p) && i < sizeof(key) - 1)
                    key[i++] = *p++;
                key[i] = 0;
            }

            p = parse_skip_ws(p);
            if (*p == ':')
                p++;
            p = parse_skip_ws(p);

            if (strcmp(key, "name") == 0) {
                p = parse_value_str(p, st->name, sizeof(st->name));
            } else if (strcmp(key, "country") == 0) {
                p = parse_value_str(p, st->country, sizeof(st->country));
            } else if (strcmp(key, "iata") == 0) {
                p = parse_value_str(p, st->iata, sizeof(st->iata));
                char *e = st->iata + strlen(st->iata) - 1;
                while (e >= st->iata && isspace(*e))
                    *e-- = 0;
            } else if (strcmp(key, "lat") == 0) {
                p = parse_value_num(p, &st->lat);
            } else if (strcmp(key, "lon") == 0) {
                p = parse_value_num(p, &st->lon);
            } else if (strcmp(key, "elev") == 0) {
                p = parse_value_num(p, &st->elev);
            } else {
                if (*p == '\'' || *p == '"') {
                    char dummy[256];
                    p = parse_quoted(p, dummy, sizeof(dummy));
                } else {
                    while (*p && *p != ',' && *p != '}')
                        p++;
                }
            }

            p = parse_skip_ws(p);
            if (*p == ',')
                p++;
        }

        if (*p == '}')
            p++;
        p = parse_skip_ws(p);
        if (*p == ',')
            p++;

        station_add(st);
        count++;
    }

    free(data);
    printf("stations: loaded %d item(s)\n", count);
    return 0;
}

static void stations_free(void) {
    for (int i = 0; i < HASH_SIZE; i++) {
        station_t *s = station_hash[i];
        while (s) {
            station_t *next = s->next;
            free(s);
            s = next;
        }
        station_hash[i] = NULL;
    }
}

static station_t *station_lookup(const char *icao) {
    unsigned h = hash_icao(icao);
    for (station_t *s = station_hash[h]; s; s = s->next)
        if (strcasecmp(s->icao, icao) == 0)
            return s;
    return NULL;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static size_t write_cb(void *ptr, size_t size, size_t nmemb, void *userdata) {
    buffer_t *buf = (buffer_t *)userdata;
    size_t total = size * nmemb;
    char *tmp = realloc(buf->data, buf->size + total + 1);
    if (!tmp)
        return 0;
    buf->data = tmp;
    memcpy(buf->data + buf->size, ptr, total);
    buf->size += total;
    buf->data[buf->size] = 0;
    return total;
}

static char *fetch_url(const char *url) {
    CURL *curl = curl_easy_init();
    if (!curl)
        return NULL;
    buffer_t buf = {NULL, 0};
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 15L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    if (res != CURLE_OK) {
        free(buf.data);
        return NULL;
    }
    return buf.data;
}

static xmlNode *xml_find(xmlNode *node, const char *name) {
    for (xmlNode *n = node; n; n = n->next)
        if (n->type == XML_ELEMENT_NODE && !strcmp((char *)n->name, name))
            return n;
    return NULL;
}
static char *xml_text(xmlNode *parent, const char *name) {
    xmlNode *n = xml_find(parent->children, name);
    if (n && n->children && n->children->type == XML_TEXT_NODE)
        return (char *)n->children->content;
    return NULL;
}
static char *xml_attr(xmlNode *node, const char *name) {
    for (xmlAttr *attr = node->properties; attr; attr = attr->next)
        if (!strcmp((char *)attr->name, name) && attr->children)
            return (char *)attr->children->content;
    return NULL;
}

static void append(char *buf, size_t bufsz, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    size_t len = strlen(buf);
    vsnprintf(buf + len, bufsz - len, fmt, ap);
    va_end(ap);
}
static void format_wind(char *out, size_t sz, xmlNode *node) {
    char *dir = xml_text(node, "wind_dir_degrees");
    char *spd = xml_text(node, "wind_speed_kt");
    if (!dir || !spd)
        return;
    int spd_i = atoi(spd);
    if (spd_i == 0) {
        append(out, sz, "Wind calm; ");
        return;
    }
    int dir_i = atoi(dir);
    if (dir_i == 0 && spd_i > 0)
        append(out, sz, "Wind variable at %skt", spd);
    else
        append(out, sz, "Wind %03d°T at %skt", dir_i, spd);
    char *gust = xml_text(node, "wind_gust_kt");
    if (gust)
        append(out, sz, " gusting %skt", gust);
    append(out, sz, "; ");
}
static void format_vis(char *out, size_t sz, xmlNode *node) {
    char *vis = xml_text(node, "visibility_statute_mi");
    if (!vis)
        return;
    double v = atof(vis) * 1609.34;
    if (v >= 5000)
        append(out, sz, "Visibility %dkm; ", (int)(v / 1000 + 0.5));
    else
        append(out, sz, "Visibility %dm; ", (int)(v / 100 + 0.5) * 100);
}
static void format_wx(char *out, size_t sz, xmlNode *node) {
    char *wx = xml_text(node, "wx_string");
    if (!wx || !*wx)
        return;
    append(out, sz, "Weather");
    if (wx[0] == '-')
        append(out, sz, " light");
    if (wx[0] == '+')
        append(out, sz, " heavy");
    if (strstr(wx, "RA"))
        append(out, sz, " rain");
    if (strstr(wx, "SN"))
        append(out, sz, " snow");
    if (strstr(wx, "DZ"))
        append(out, sz, " drizzle");
    if (strstr(wx, "FG"))
        append(out, sz, " fog");
    if (strstr(wx, "BR"))
        append(out, sz, " mist");
    if (strstr(wx, "HZ"))
        append(out, sz, " haze");
    if (strstr(wx, "TS"))
        append(out, sz, " thunderstorm");
    if (strstr(wx, "SH"))
        append(out, sz, " showers");
    if (strstr(wx, "FZ"))
        append(out, sz, " freezing");
    append(out, sz, "; ");
}
static void format_sky(char *out, size_t sz, xmlNode *node) {
    int found = 0;
    for (xmlNode *n = node->children; n; n = n->next) {
        if (n->type == XML_ELEMENT_NODE && !strcmp((char *)n->name, "sky_condition")) {
            if (!found) {
                append(out, sz, "Sky ");
                found = 1;
            }
            char *cover = xml_attr(n, "sky_cover");
            char *base = xml_attr(n, "cloud_base_ft_agl");
            if (cover) {
                if (!strcmp(cover, "CLR") || !strcmp(cover, "SKC"))
                    append(out, sz, "clear");
                else if (!strcmp(cover, "FEW"))
                    append(out, sz, "few");
                else if (!strcmp(cover, "SCT"))
                    append(out, sz, "scattered");
                else if (!strcmp(cover, "BKN"))
                    append(out, sz, "broken");
                else if (!strcmp(cover, "OVC"))
                    append(out, sz, "overcast");
                if (base && strcmp(cover, "CLR") && strcmp(cover, "SKC"))
                    append(out, sz, " %sft", base);
                append(out, sz, ", ");
            }
        }
    }
    if (found) {
        out[strlen(out) - 2] = ';';
        out[strlen(out) - 1] = ' ';
    }
}
static void format_temp(char *out, size_t sz, xmlNode *node) {
    char *temp = xml_text(node, "temp_c");
    if (temp)
        append(out, sz, "Temp %d°C; ", (int)(atof(temp) + 0.5));
    char *dewp = xml_text(node, "dewpoint_c");
    if (dewp)
        append(out, sz, "Dewpoint %d°C; ", (int)(atof(dewp) + 0.5));
}
static void format_press(char *out, size_t sz, xmlNode *node) {
    char *altim = xml_text(node, "altim_in_hg");
    if (altim)
        append(out, sz, "QNH %d hPa", (int)(0.5 + 33.8639 * atof(altim)));
}
static void format_time(char *out, size_t sz, const char *iso) {
    struct tm tm = {0};
    if (sscanf(iso, "%d-%d-%dT%d:%d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min) == 5) {
        static const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
        snprintf(out, sz, "%d %s %02d%02dZ", tm.tm_mday, months[tm.tm_mon - 1], tm.tm_hour, tm.tm_min);
    }
}

// -----------------------------------------------------------------------------------------------------------------------------------------

static cJSON *process_metar(const char *xml_data, const char *icao) {
    xmlDoc *doc = xmlReadMemory(xml_data, strlen(xml_data), NULL, NULL, 0);
    if (!doc)
        return NULL;
    xmlNode *root = xmlDocGetRootElement(doc);
    xmlNode *data = xml_find(root->children, "data");
    xmlNode *metar = data ? xml_find(data->children, "METAR") : NULL;
    if (!metar) {
        xmlFreeDoc(doc);
        return NULL;
    }

    char text[2048] = "";

    char timestr[64] = "";
    char *obs_time = xml_text(metar, "observation_time");
    if (obs_time)
        format_time(timestr, sizeof(timestr), obs_time);
    station_t *st = station_lookup(icao);
    if (st && st->name[0]) {
        char name[MAX_NAME];
        strncpy(name, st->name, sizeof(name));
        name[0] = toupper(name[0]);
        snprintf(text, sizeof(text), "METAR for %s (%s) issued %s\n", name, icao, timestr);
    } else {
        snprintf(text, sizeof(text), "METAR for %s issued %s\n", icao, timestr);
    }

    format_wind(text, sizeof(text), metar);
    format_vis(text, sizeof(text), metar);
    format_wx(text, sizeof(text), metar);
    format_sky(text, sizeof(text), metar);
    format_temp(text, sizeof(text), metar);
    format_press(text, sizeof(text), metar);
    char *flight_cat = xml_text(metar, "flight_category");
    if (flight_cat)
        append(text, sizeof(text), "; Category %s", flight_cat);

    cJSON *json = cJSON_CreateObject();
    char *raw = xml_text(metar, "raw_text");
    if (raw)
        cJSON_AddStringToObject(json, "raw", raw);
    cJSON_AddStringToObject(json, "text", text);
    xmlFreeDoc(doc);
    return json;
}

static cJSON *process_taf(const char *xml_data, const char *icao) {
    xmlDoc *doc = xmlReadMemory(xml_data, strlen(xml_data), NULL, NULL, 0);
    if (!doc)
        return NULL;
    xmlNode *root = xmlDocGetRootElement(doc);
    xmlNode *data = xml_find(root->children, "data");
    xmlNode *taf = data ? xml_find(data->children, "TAF") : NULL;
    if (!taf) {
        xmlFreeDoc(doc);
        return NULL;
    }

    char text[4096] = "";

    char t1[64] = "", t2[64] = "", t3[64] = "";
    char *issue = xml_text(taf, "issue_time");
    if (issue)
        format_time(t1, sizeof(t1), issue);
    char *valid_from = xml_text(taf, "valid_time_from");
    if (valid_from)
        format_time(t2, sizeof(t2), valid_from);
    char *valid_to = xml_text(taf, "valid_time_to");
    if (valid_to)
        format_time(t3, sizeof(t3), valid_to);
    station_t *st = station_lookup(icao);
    if (st && st->name[0]) {
        char name[MAX_NAME];
        strncpy(name, st->name, sizeof(name));
        name[0] = toupper(name[0]);
        snprintf(text, sizeof(text), "TAF for %s (%s) issued %s valid %s to %s\n", name, icao, t1, t2, t3);
    } else {
        snprintf(text, sizeof(text), "TAF for %s issued %s valid %s to %s\n", icao, t1, t2, t3);
    }

    for (xmlNode *fc = taf->children; fc; fc = fc->next) {
        if (fc->type == XML_ELEMENT_NODE && !strcmp((char *)fc->name, "forecast")) {
            char *from = xml_text(fc, "fcst_time_from");
            char *to = xml_text(fc, "fcst_time_to");
            char *change = xml_text(fc, "change_indicator");
            char tf[32] = "", tt[32] = "";
            if (from)
                format_time(tf, sizeof(tf), from);
            if (to)
                format_time(tt, sizeof(tt), to);
            if (change)
                append(text, sizeof(text), "%s ", change);
            append(text, sizeof(text), "%s-%s ", tf, tt);
            format_wind(text, sizeof(text), fc);
            format_vis(text, sizeof(text), fc);
            format_wx(text, sizeof(text), fc);
            format_sky(text, sizeof(text), fc);
            append(text, sizeof(text), "\n");
        }
    }

    cJSON *json = cJSON_CreateObject();
    char *raw = xml_text(taf, "raw_text");
    if (raw)
        cJSON_AddStringToObject(json, "raw", raw);
    cJSON_AddStringToObject(json, "text", text);
    xmlFreeDoc(doc);
    return json;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static void fetch_and_publish(airport_t *ap) {
    char timestamp[32];
    time_t now = time(NULL);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));

    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "timestamp", timestamp);

    cJSON *airport = cJSON_AddObjectToObject(root, "airport");
    cJSON_AddStringToObject(airport, "icao", ap->icao);

    station_t *st = station_lookup(ap->icao);
    if (st) {
        if (st->name[0])
            cJSON_AddStringToObject(airport, "name", st->name);
        if (st->country[0])
            cJSON_AddStringToObject(airport, "country", st->country);
        if (st->iata[0])
            cJSON_AddStringToObject(airport, "iata", st->iata);
        cJSON_AddNumberToObject(airport, "lat", st->lat);
        cJSON_AddNumberToObject(airport, "lon", st->lon);
        cJSON_AddNumberToObject(airport, "elev_km", st->elev);
    }

    if (ap->fetch_metar) {
        char url[MAX_URL];
        snprintf(url, sizeof(url), "https://aviationweather.gov/api/data/metar?format=xml&taf=false&hours=1&ids=%s", ap->icao);
        char *xml = fetch_url(url);
        if (xml) {
            cJSON *metar = process_metar(xml, ap->icao);
            if (metar)
                cJSON_AddItemToObject(root, "metar", metar);
            free(xml);
        }
    }

    if (ap->fetch_taf) {
        char url[MAX_URL];
        snprintf(url, sizeof(url), "https://aviationweather.gov/api/data/taf?format=xml&ids=%s", ap->icao);
        char *xml = fetch_url(url);
        if (xml) {
            cJSON *taf = process_taf(xml, ap->icao);
            if (taf)
                cJSON_AddItemToObject(root, "taf", taf);
            free(xml);
        }
    }

    char *payload = cJSON_PrintUnformatted(root);
    char topic[MAX_TOPIC];
    snprintf(topic, sizeof(topic), "%s/%s", cfg.topic_prefix, ap->icao);
    printf("airports: publishing '%s' to '%s'\n", ap->icao, topic);
    mosquitto_publish(mosq, NULL, topic, strlen(payload), payload, 0, true);
    free(payload);
    cJSON_Delete(root);
    ap->last_fetch = now;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static int config_load(const char *path) {

    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "config: cannot open file: %s\n", path);
        return -1;
    }
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data = malloc(len + 1);
    fread(data, 1, len, f);
    data[len] = 0;
    fclose(f);

    cJSON *json = cJSON_Parse(data);
    free(data);
    if (!json) {
        fprintf(stderr, "config: parse error\n");
        return -1;
    }

    cJSON *mqtt = cJSON_GetObjectItem(json, "mqtt");
    if (mqtt) {
        const char *s;
        if ((s = cJSON_GetStringValue(cJSON_GetObjectItem(mqtt, "broker"))))
            strncpy(cfg.broker, s, sizeof(cfg.broker) - 1);
        if ((s = cJSON_GetStringValue(cJSON_GetObjectItem(mqtt, "client_id"))))
            strncpy(cfg.client_id, s, sizeof(cfg.client_id) - 1);
        if ((s = cJSON_GetStringValue(cJSON_GetObjectItem(mqtt, "topic_prefix"))))
            strncpy(cfg.topic_prefix, s, sizeof(cfg.topic_prefix) - 1);
        if ((s = cJSON_GetStringValue(cJSON_GetObjectItem(mqtt, "username"))))
            strncpy(cfg.username, s, sizeof(cfg.username) - 1);
        if ((s = cJSON_GetStringValue(cJSON_GetObjectItem(mqtt, "password"))))
            strncpy(cfg.password, s, sizeof(cfg.password) - 1);
        if ((s = cJSON_GetStringValue(cJSON_GetObjectItem(mqtt, "stations_file"))))
            strncpy(cfg.stations_file, s, sizeof(cfg.stations_file) - 1);
    }

    cJSON *defaults = cJSON_GetObjectItem(json, "defaults");
    if (defaults) {
        cJSON *v;
        if ((v = cJSON_GetObjectItem(defaults, "fetch_metar")))
            cfg.default_metar = cJSON_IsTrue(v);
        if ((v = cJSON_GetObjectItem(defaults, "fetch_taf")))
            cfg.default_taf = cJSON_IsTrue(v);
        if ((v = cJSON_GetObjectItem(defaults, "interval_minutes")))
            cfg.default_interval = v->valueint;
    }

    cJSON *airports = cJSON_GetObjectItem(json, "airports");
    if (airports && cJSON_IsArray(airports)) {
        cJSON *ap;
        cJSON_ArrayForEach(ap, airports) {
            if (cfg.airport_count >= MAX_AIRPORTS)
                break;
            airport_t *a = &cfg.airports[cfg.airport_count];
            cJSON *v;
            const char *s;
            if ((s = cJSON_GetStringValue(cJSON_GetObjectItem(ap, "icao")))) {
                strncpy(a->icao, s, MAX_ICAO - 1);
                for (char *p = a->icao; *p; p++)
                    *p = toupper(*p);
            }
            a->fetch_metar = (v = cJSON_GetObjectItem(ap, "fetch_metar")) ? cJSON_IsTrue(v) : cfg.default_metar;
            a->fetch_taf = (v = cJSON_GetObjectItem(ap, "fetch_taf")) ? cJSON_IsTrue(v) : cfg.default_taf;
            a->interval = (v = cJSON_GetObjectItem(ap, "interval_minutes")) ? v->valueint : cfg.default_interval;
            a->last_fetch = 0;
            cfg.airport_count++;
        }
    }

    cJSON_Delete(json);
    return 0;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static void signal_handler(int sig) {
    (void)sig;
    running = 0;
}

int main(int argc, char **argv) {
    const char *config_path = argc > 1 ? argv[1] : "avw2mqtt.conf";

    memset(&cfg, 0, sizeof(cfg));
    strcpy(cfg.broker, "localhost");
    strcpy(cfg.client_id, "avw2mqtt");
    strcpy(cfg.topic_prefix, "weather/aviation");
    cfg.default_metar = cfg.default_taf = 1;
    cfg.default_interval = 10;

    if (config_load(config_path) < 0)
        return EXIT_FAILURE;
    if (cfg.airport_count == 0) {
        fprintf(stderr, "airports: none configured\n");
        return EXIT_FAILURE;
    }
    printf("airports: loaded %d item(s)\n", cfg.airport_count);

    if (cfg.stations_file[0])
        if (stations_load(cfg.stations_file) < 0)
            fprintf(stderr, "stations: none loaded\n");

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    curl_global_init(CURL_GLOBAL_DEFAULT);

    mosquitto_lib_init();
    mosq = mosquitto_new(cfg.client_id, true, NULL);
    if (!mosq) {
        fprintf(stderr, "mqtt: mosquitto_new failed\n");
        return EXIT_FAILURE;
    }
    if (cfg.username[0])
        mosquitto_username_pw_set(mosq, cfg.username, cfg.password);
    char host[256];
    int port = 1883;
    strncpy(host, cfg.broker, sizeof(host));
    char *h = host;
    if (strncmp(h, "mqtt://", 7) == 0)
        h += 7;
    char *colon = strchr(h, ':');
    if (colon) {
        *colon = 0;
        port = atoi(colon + 1);
    }
    printf("mqtt: connecting to %s:%d\n", h, port);
    if (mosquitto_connect(mosq, h, port, 60) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "mqtt: connect failed\n");
        return EXIT_FAILURE;
    }
    mosquitto_loop_start(mosq);

    printf("running ... press Ctrl+C to stop.\n");
    while (running) {
        time_t now = time(NULL);
        for (int i = 0; i < cfg.airport_count && running; i++) {
            airport_t *ap = &cfg.airports[i];
            if (now - ap->last_fetch >= ap->interval * 60)
                fetch_and_publish(ap);
        }
        sleep(1);
    }
    printf("\nstopping ...\n");

    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    curl_global_cleanup();
    stations_free();
    return EXIT_SUCCESS;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------
