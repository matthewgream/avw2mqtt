// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

#define _GNU_SOURCE
#include <ctype.h>
#include <getopt.h>
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

#define LEARN_SAMPLES 5
#define METAR_CAP_MINUTES 35
#define TAF_CAP_MINUTES 65
#define SLACK_SECONDS 60

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

typedef struct station {
    char icao[MAX_ICAO];
    char name[MAX_NAME];
    char country[MAX_COUNTRY];
    char iata[MAX_IATA];
    double lat, lon, elev;
} station_t;

typedef struct {
    time_t samples[LEARN_SAMPLES];
    int sample_count;
    int learned_period;
    time_t last_issued;
    time_t next_fetch;
} schedule_t;

typedef struct {
    char icao[MAX_ICAO];
    int fetch_metar, fetch_taf, interval;
    time_t last_fetch;
    schedule_t sched_metar, sched_taf;
    cJSON *json;
    // station data (cached)
    char name[MAX_NAME];
    char country[MAX_COUNTRY];
    char iata[MAX_IATA];
    double lat, lon, elev;
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

typedef struct {
    int debug;
    int header;
    int all;
    int learn;
    int split;
    const char *config_path;
} options_t;

static volatile int running = 1;
static struct mosquitto *mosq = NULL;

static config_t cfg;
static options_t opts = {0, 0, 0, 1, 0, "avw2mqtt.conf"};

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static void debug(const char *fmt, ...) {
    if (!opts.debug)
        return;
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "[debug] ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

static char *read_file(const char *path, const char *type) {
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "%s: file cannot cannot be opened: %s\n", type, path);
        return NULL;
    }
    fseek(f, 0, SEEK_END);
    size_t len = (size_t)ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data = malloc(len + 1);
    if (!data) {
        fclose(f);
        fprintf(stderr, "%s: file too large: %s\n", type, path);
        return NULL;
    }
    fread(data, 1, len, f);
    data[len] = 0;
    fclose(f);
    return data;
}

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

typedef void (*station_cb)(const station_t *st, void *userdata);

static int stations_load(const char *path, station_cb cb, void *userdata) {
    char *data = read_file(path, "stations");
    if (!data)
        return -1;

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

        station_t st = {0};
        for (char *c = icao; *c; c++)
            *c = (char)toupper(*c);
        memcpy(st.icao, icao, sizeof(st.icao));

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
                p = parse_value_str(p, st.name, sizeof(st.name));
            } else if (strcmp(key, "country") == 0) {
                p = parse_value_str(p, st.country, sizeof(st.country));
            } else if (strcmp(key, "iata") == 0) {
                p = parse_value_str(p, st.iata, sizeof(st.iata));
                char *e = st.iata + strlen(st.iata) - 1;
                while (e >= st.iata && isspace(*e))
                    *e-- = 0;
            } else if (strcmp(key, "lat") == 0) {
                p = parse_value_num(p, &st.lat);
            } else if (strcmp(key, "lon") == 0) {
                p = parse_value_num(p, &st.lon);
            } else if (strcmp(key, "elev") == 0) {
                p = parse_value_num(p, &st.elev);
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

        cb(&st, userdata);
        count++;
    }

    free(data);
    debug("stations: parsed %d item(s)", count);
    return 0;
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
    debug("fetch: %s", url);
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
        debug("fetch: failed (%s)", curl_easy_strerror(res));
        free(buf.data);
        return NULL;
    }
    debug("fetch: received %zu bytes", buf.size);
    return buf.data;
}

static xmlNode *xml_find(xmlNode *node, const char *name) {
    for (xmlNode *n = node; n; n = n->next)
        if (n->type == XML_ELEMENT_NODE && !strcmp((const char *)n->name, name))
            return n;
    return NULL;
}
static char *xml_text(xmlNode *parent, const char *name) {
    xmlNode *n = xml_find(parent->children, name);
    if (n && n->children && (n->children->type == XML_TEXT_NODE || n->children->type == XML_CDATA_SECTION_NODE))
        return (char *)n->children->content;
    return NULL;
}
static char *xml_attr(xmlNode *node, const char *name) {
    for (xmlAttr *attr = node->properties; attr; attr = attr->next)
        if (!strcmp((const char *)attr->name, name) && attr->children)
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

static time_t parse_iso_time(const char *iso) {
    if (!iso)
        return 0;
    struct tm tm = {0};
    if (sscanf(iso, "%d-%d-%dT%d:%d:%d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec) >= 5) {
        tm.tm_year -= 1900;
        tm.tm_mon -= 1;
        return timegm(&tm);
    }
    return 0;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static void format_time(char *out, size_t sz, const char *iso) {
    struct tm tm = {0};
    if (sscanf(iso, "%d-%d-%dT%d:%d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min) == 5) {
        static const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
        append(out, sz, "%s-%d-%02d%02dZ", months[tm.tm_mon - 1], tm.tm_mday, tm.tm_hour, tm.tm_min);
    }
}

static void format_wind(char *out, size_t sz, xmlNode *node) {
    const char *dir = xml_text(node, "wind_dir_degrees");
    const char *spd = xml_text(node, "wind_speed_kt");
    if (!dir || !spd)
        return;
    const int spd_i = atoi(spd);
    if (spd_i == 0) {
        append(out, sz, "Wind calm; ");
        return;
    }
    const int dir_i = atoi(dir);
    if (dir_i == 0 && spd_i > 0)
        append(out, sz, "Wind variable at %skt", spd);
    else
        append(out, sz, "Wind %03d°T at %skt", dir_i, spd);
    const char *gust = xml_text(node, "wind_gust_kt");
    if (gust)
        append(out, sz, " gusting %skt", gust);
    append(out, sz, "; ");
}
static void format_vis(char *out, size_t sz, xmlNode *node) {
    const char *vis = xml_text(node, "visibility_statute_mi");
    if (!vis)
        return;
    const double v = atof(vis) * 1609.34;
    if (v >= 5000)
        append(out, sz, "Visibility %dkm; ", (int)(v / 1000 + 0.5));
    else
        append(out, sz, "Visibility %dm; ", (int)(v / 100 + 0.5) * 100);
}
static void format_wx(char *out, size_t sz, xmlNode *node) {
    const char *wx = xml_text(node, "wx_string");
    if (!wx || !*wx)
        return;
    append(out, sz, "Weather");
    if (wx[0] == '-')
        append(out, sz, " light");
    if (wx[0] == '+')
        append(out, sz, " heavy");
    if (strstr(wx, "NSW"))
        append(out, sz, " no significant");
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
    if (strstr(wx, "MI"))
        append(out, sz, " shallow");
    if (strstr(wx, "BC"))
        append(out, sz, " patches");
    if (strstr(wx, "PR"))
        append(out, sz, " partial");
    if (strstr(wx, "DR"))
        append(out, sz, " drifting");
    if (strstr(wx, "BL"))
        append(out, sz, " blowing");
    if (strstr(wx, "PL"))
        append(out, sz, " ice pellets");
    if (strstr(wx, "GR"))
        append(out, sz, " hail");
    if (strstr(wx, "GS"))
        append(out, sz, " small hail");
    if (strstr(wx, "SG"))
        append(out, sz, " snow grains");
    if (strstr(wx, "IC"))
        append(out, sz, " ice crystals");
    if (strstr(wx, "UP"))
        append(out, sz, " unknown precip");
    if (strstr(wx, "VA"))
        append(out, sz, " volcanic ash");
    if (strstr(wx, "DU"))
        append(out, sz, " dust");
    if (strstr(wx, "SA"))
        append(out, sz, " sand");
    if (strstr(wx, "PY"))
        append(out, sz, " spray");
    if (strstr(wx, "PO"))
        append(out, sz, " dust whirls");
    if (strstr(wx, "SQ"))
        append(out, sz, " squalls");
    if (strstr(wx, "FC"))
        append(out, sz, " funnel cloud");
    if (strstr(wx, "SS"))
        append(out, sz, " sandstorm");
    if (strstr(wx, "DS"))
        append(out, sz, " duststorm");
    if (strstr(wx, "VC"))
        append(out, sz, " in vicinity");
    append(out, sz, "; ");
}
static void format_sky(char *out, size_t sz, xmlNode *node) {
    int found = 0;
    const char *vv = xml_text(node, "vert_vis_ft");
    if (vv) {
        append(out, sz, "Sky obscured, vertical visibility %sft; ", vv);
        return;
    }
    for (xmlNode *n = node->children; n; n = n->next) {
        if (n->type != XML_ELEMENT_NODE || strcmp((const char *)n->name, "sky_condition"))
            continue;
        const char *cover = xml_attr(n, "sky_cover");
        if (!cover)
            continue;
        const char *cover_txt = NULL;
        int has_base = 1;
        if (!strcmp(cover, "CLR") || !strcmp(cover, "SKC")) {
            cover_txt = "clear";
            has_base = 0;
        } else if (!strcmp(cover, "NCD")) {
            cover_txt = "no cloud detected";
            has_base = 0;
        } else if (!strcmp(cover, "NSC")) {
            cover_txt = "no significant cloud";
            has_base = 0;
        } else if (!strcmp(cover, "CAVOK")) {
            cover_txt = "cavok";
            has_base = 0;
        } else if (!strcmp(cover, "VV"))
            cover_txt = "vertical visibility";
        else if (!strcmp(cover, "FEW"))
            cover_txt = "few";
        else if (!strcmp(cover, "SCT"))
            cover_txt = "scattered";
        else if (!strcmp(cover, "BKN"))
            cover_txt = "broken";
        else if (!strcmp(cover, "OVC"))
            cover_txt = "overcast";
        else
            continue;
        if (!found) {
            append(out, sz, "Sky ");
            found = 1;
        }
        append(out, sz, "%s", cover_txt);
        const char *base = xml_attr(n, "cloud_base_ft_agl");
        if (base && has_base)
            append(out, sz, " %sft", base);
        const char *cloud_type = xml_attr(n, "cloud_type");
        if (cloud_type) {
            if (!strcmp(cloud_type, "CB"))
                append(out, sz, " CB");
            else if (!strcmp(cloud_type, "TCU"))
                append(out, sz, " TCU");
        }
        append(out, sz, ", ");
    }
    if (found) {
        out[strlen(out) - 2] = ';';
        out[strlen(out) - 1] = ' ';
    }
}
static void format_temp(char *out, size_t sz, xmlNode *node) {
    const char *temp = xml_text(node, "temp_c");
    if (temp)
        append(out, sz, "Temp %d°C; ", atoi(temp));
    const char *dewp = xml_text(node, "dewpoint_c");
    if (dewp)
        append(out, sz, "Dewpoint %d°C; ", atoi(dewp));
}
static void format_press(char *out, size_t sz, xmlNode *node) {
    const char *altim = xml_text(node, "altim_in_hg");
    if (altim)
        append(out, sz, "QNH %d hPa; ", (int)(0.5 + 33.8639 * atof(altim)));
}
static void format_forecast_time(char *out, size_t sz, xmlNode *node) {
    const char *from = xml_text(node, "fcst_time_from");
    const char *to = xml_text(node, "fcst_time_to");
    char tf[32] = "", tt[32] = "";
    if (from)
        format_time(tf, sizeof(tf), from);
    if (to)
        format_time(tt, sizeof(tt), to);
    append(out, sz, "%s/%s ", tf, tt);
}
static void format_category(char *out, size_t sz, xmlNode *node) {
    const char *cat = xml_text(node, "flight_category");
    if (cat)
        append(out, sz, "%s; ", cat);
}
static void format_change(char *out, size_t sz, xmlNode *node) {
    const char *change = xml_text(node, "change_indicator");
    if (change) {
        if (!strcmp(change, "FM"))
            append(out, sz, "FROM ");
        else if (!strcmp(change, "BECMG"))
            append(out, sz, "BECOMING ");
        else
            append(out, sz, "%s ", change);
    }
}
static void format_end(char *out, size_t sz) {
    if (strlen(out) > 2)
        if (out[strlen(out) - 1] == ' ' && out[strlen(out) - 2] == ';')
            out[strlen(out) - 2] = '\0';
    append(out, sz, "\n");
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static void schedule_add_sample(schedule_t *sched, time_t issued) {
    if (issued == 0)
        return;
    if (sched->sample_count >= LEARN_SAMPLES) {
        memmove(&sched->samples[0], &sched->samples[1], (LEARN_SAMPLES - 1) * sizeof(time_t));
        sched->sample_count = LEARN_SAMPLES - 1;
    }
    sched->samples[sched->sample_count++] = issued;
}

static void schedule_learn(schedule_t *sched, const char *icao, const char *type, int cap_minutes) {
    if (sched->sample_count < 2)
        return;
    int deltas[LEARN_SAMPLES - 1];
    int delta_count = 0;
    for (int i = 1; i < sched->sample_count; i++) {
        const int delta = (int)(sched->samples[i] - sched->samples[i - 1]);
        if (delta > 0)
            deltas[delta_count++] = delta;
    }
    if (delta_count == 0)
        return;
    int min_delta = deltas[0];
    for (int i = 1; i < delta_count; i++)
        if (deltas[i] < min_delta)
            min_delta = deltas[i];
    int consistent = 1;
    for (int i = 0; i < delta_count; i++) {
        int remainder = deltas[i] % min_delta;
        if (remainder > 120 && remainder < min_delta - 120) {
            consistent = 0;
            break;
        }
    }
    if (consistent && sched->sample_count >= LEARN_SAMPLES) {
        sched->learned_period = min_delta;
        debug("[%s] %s learned period: %d seconds (%d minutes)", icao, type, min_delta, min_delta / 60);
    }
    const int cap_seconds = cap_minutes * 60;
    if (sched->learned_period > cap_seconds) {
        debug("[%s] %s period capped from %d to %d seconds", icao, type, sched->learned_period, cap_seconds);
        sched->learned_period = cap_seconds;
    }
}

static void schedule_update_next(schedule_t *sched, const char *icao, const char *type, int default_interval, int cap_minutes) {
    const time_t now = time(NULL);
    if (sched->learned_period > 0 && sched->last_issued > 0) {
        time_t next = sched->last_issued + sched->learned_period;
        while (next <= now)
            next += sched->learned_period;
        sched->next_fetch = next + SLACK_SECONDS;
        debug("[%s] %s next fetch at %ld (in %ld seconds)", icao, type, sched->next_fetch, sched->next_fetch - now);
    } else {
        const int cap = cap_minutes * 60;
        int interval = default_interval * 60;
        if (interval > cap)
            interval = cap;
        sched->next_fetch = now + interval;
        debug("[%s] %s next fetch in %d seconds (default)", icao, type, interval);
    }
}

static void schedule_missed(schedule_t *sched, const char *icao, const char *type) {
    if (sched->sample_count > 2) {
        debug("[%s] %s unexpected timing, reducing samples %d -> 2", icao, type, sched->sample_count);
        sched->samples[0] = sched->samples[sched->sample_count - 2];
        sched->samples[1] = sched->samples[sched->sample_count - 1];
        sched->sample_count = 2;
    }
    sched->learned_period = 0;
}

// -----------------------------------------------------------------------------------------------------------------------------------------

static cJSON *process_metar(const char *xml_data, const airport_t *ap, time_t *out_observed) {
    xmlDoc *doc = xmlReadMemory(xml_data, (int)strlen(xml_data), NULL, NULL, 0);
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
    const char *observed = xml_text(metar, "observation_time");
    if (observed)
        format_time(timestr, sizeof(timestr), observed);
    if (out_observed)
        *out_observed = parse_iso_time(observed);

    if (opts.header) {
        if (ap->name[0]) {
            char name[MAX_NAME];
            strncpy(name, ap->name, sizeof(name));
            name[0] = (char)toupper(name[0]);
            append(text, sizeof(text), "METAR for %s (%s) issued %s\n", name, ap->icao, timestr);
        } else {
            append(text, sizeof(text), "METAR for %s issued %s\n", ap->icao, timestr);
        }
    } else {
        append(text, sizeof(text), "issued %s\n", timestr);
    }

    format_wind(text, sizeof(text), metar);
    format_vis(text, sizeof(text), metar);
    format_wx(text, sizeof(text), metar);
    format_sky(text, sizeof(text), metar);
    format_temp(text, sizeof(text), metar);
    format_press(text, sizeof(text), metar);
    format_category(text, sizeof(text), metar);
    format_end(text, sizeof(text));

    const char *raw = xml_text(metar, "raw_text");
    debug("[%s] METAR raw: %s", ap->icao, raw ? raw : "(none)");
    debug("[%s] METAR text: %s", ap->icao, text);

    cJSON *json = cJSON_CreateObject();
    if (observed)
        cJSON_AddStringToObject(json, "observed", observed);
    if (raw)
        cJSON_AddStringToObject(json, "raw", raw);
    cJSON_AddStringToObject(json, "text", text);
    xmlFreeDoc(doc);
    return json;
}

static cJSON *process_taf(const char *xml_data, const airport_t *ap, time_t *out_issued) {
    xmlDoc *doc = xmlReadMemory(xml_data, (int)strlen(xml_data), NULL, NULL, 0);
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
    const char *issued = xml_text(taf, "issue_time");
    if (issued)
        format_time(t1, sizeof(t1), issued);
    if (out_issued)
        *out_issued = parse_iso_time(issued);
    const char *valid_from = xml_text(taf, "valid_time_from");
    if (valid_from)
        format_time(t2, sizeof(t2), valid_from);
    const char *valid_to = xml_text(taf, "valid_time_to");
    if (valid_to)
        format_time(t3, sizeof(t3), valid_to);

    if (opts.header) {
        if (ap->name[0]) {
            char name[MAX_NAME];
            strncpy(name, ap->name, sizeof(name));
            name[0] = (char)toupper(name[0]);
            append(text, sizeof(text), "TAF for %s (%s) issued %s valid %s to %s\n", name, ap->icao, t1, t2, t3);
        } else {
            append(text, sizeof(text), "TAF for %s issued %s valid %s to %s\n", ap->icao, t1, t2, t3);
        }
    } else {
        append(text, sizeof(text), "issued %s valid %s to %s\n", t1, t2, t3);
    }

    for (xmlNode *fc = taf->children; fc; fc = fc->next)
        if (fc->type == XML_ELEMENT_NODE && !strcmp((const char *)fc->name, "forecast")) {
            format_forecast_time(text, sizeof(text), fc);
            format_change(text, sizeof(text), fc);
            format_wind(text, sizeof(text), fc);
            format_vis(text, sizeof(text), fc);
            format_wx(text, sizeof(text), fc);
            format_sky(text, sizeof(text), fc);
            format_end(text, sizeof(text));
        }

    const char *raw = xml_text(taf, "raw_text");
    debug("[%s] TAF raw: %s", ap->icao, raw ? raw : "(none)");
    debug("[%s] TAF text: %s", ap->icao, text);

    cJSON *json = cJSON_CreateObject();
    if (issued)
        cJSON_AddStringToObject(json, "issued", issued);
    if (raw)
        cJSON_AddStringToObject(json, "raw", raw);
    cJSON_AddStringToObject(json, "text", text);
    xmlFreeDoc(doc);
    return json;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static void publish_payload(const airport_t *ap, const cJSON *root, const char *suffix) {
    char *payload = cJSON_PrintUnformatted(root);
    char topic[MAX_TOPIC];
    snprintf(topic, sizeof(topic), "%s/%s%s%s", cfg.topic_prefix, ap->icao, suffix ? "/" : "", suffix ? suffix : "");
    printf("publish: %s to %s\n", ap->icao, topic);
    mosquitto_publish(mosq, NULL, topic, (int)strlen(payload), payload, 0, true);
    free(payload);
}
static void publish_type(airport_t *ap, const char *timestamp, const cJSON *object, const char *name) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "timestamp", timestamp);
    cJSON_AddItemToObject(root, "airport", cJSON_Duplicate(ap->json, 1));
    cJSON_AddItemToObject(root, name, cJSON_Duplicate(object, 1));
    publish_payload(ap, root, name);
    cJSON_Delete(root);
}
static void publish_split(airport_t *ap, const char *timestamp, const cJSON *metar, int metar_changed, const cJSON *taf, int taf_changed) {
    if (metar && metar_changed)
        publish_type(ap, timestamp, metar, "metar");
    if (taf && taf_changed)
        publish_type(ap, timestamp, taf, "taf");
}

static void publish_combined(airport_t *ap, const char *timestamp, const cJSON *metar, const cJSON *taf) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "timestamp", timestamp);
    cJSON_AddItemToObject(root, "airport", cJSON_Duplicate(ap->json, 1));
    if (metar)
        cJSON_AddItemToObject(root, "metar", cJSON_Duplicate(metar, 1));
    if (taf)
        cJSON_AddItemToObject(root, "taf", cJSON_Duplicate(taf, 1));
    publish_payload(ap, root, NULL);
    cJSON_Delete(root);
}

static void fetch_and_publish(airport_t *ap) {
    char timestamp[32];
    const time_t now = time(NULL);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));

    cJSON *metar = NULL, *taf = NULL;
    int metar_changed = 0, taf_changed = 0;
    time_t observed = 0, issued = 0;

    if (ap->fetch_metar) {
        char url[MAX_URL];
        snprintf(url, sizeof(url), "https://aviationweather.gov/api/data/metar?format=xml&taf=false&ids=%s", ap->icao);
        char *xml = fetch_url(url);
        if (xml) {
            metar = process_metar(xml, ap, &observed);
            free(xml);
        }
        if (metar) {
            if (opts.all) {
                metar_changed = 1;
            } else if (observed != ap->sched_metar.last_issued) {
                if (ap->sched_metar.last_issued != 0 && observed < ap->sched_metar.next_fetch - SLACK_SECONDS)
                    schedule_missed(&ap->sched_metar, ap->icao, "METAR");
                metar_changed = 1;
                debug("[%s] METAR changed: %ld -> %ld", ap->icao, ap->sched_metar.last_issued, observed);
                if (opts.learn) {
                    schedule_add_sample(&ap->sched_metar, observed);
                    schedule_learn(&ap->sched_metar, ap->icao, "METAR", METAR_CAP_MINUTES);
                }
                ap->sched_metar.last_issued = observed;
            } else {
                debug("[%s] METAR unchanged", ap->icao);
            }
            if (opts.learn)
                schedule_update_next(&ap->sched_metar, ap->icao, "METAR", ap->interval, METAR_CAP_MINUTES);
        }
    }

    if (ap->fetch_taf) {
        char url[MAX_URL];
        snprintf(url, sizeof(url), "https://aviationweather.gov/api/data/taf?format=xml&ids=%s", ap->icao);
        char *xml = fetch_url(url);
        if (xml) {
            taf = process_taf(xml, ap, &issued);
            free(xml);
        }
        if (taf) {
            if (opts.all) {
                taf_changed = 1;
            } else if (issued != ap->sched_taf.last_issued) {
                if (ap->sched_taf.last_issued != 0 && issued < ap->sched_taf.next_fetch - SLACK_SECONDS)
                    schedule_missed(&ap->sched_taf, ap->icao, "TAF");
                taf_changed = 1;
                debug("[%s] TAF changed: %ld -> %ld", ap->icao, ap->sched_taf.last_issued, issued);
                if (opts.learn) {
                    schedule_add_sample(&ap->sched_taf, issued);
                    schedule_learn(&ap->sched_taf, ap->icao, "TAF", TAF_CAP_MINUTES);
                }
                ap->sched_taf.last_issued = issued;
            } else {
                debug("[%s] TAF unchanged", ap->icao);
            }
            if (opts.learn)
                schedule_update_next(&ap->sched_taf, ap->icao, "TAF", ap->interval, TAF_CAP_MINUTES);
        }
    }

    if (opts.split) {
        publish_split(ap, timestamp, metar, metar_changed, taf, taf_changed);
    } else {
        if (metar_changed || taf_changed)
            publish_combined(ap, timestamp, metar, taf);
        else
            debug("[%s] nothing to publish", ap->icao);
    }

    if (metar)
        cJSON_Delete(metar);
    if (taf)
        cJSON_Delete(taf);
    ap->last_fetch = now;
}

static int should_fetch(const airport_t *ap) {
    time_t now = time(NULL);
    if (opts.all)
        return (now - ap->last_fetch >= ap->interval * 60);
    int fetch = 0;
    if (ap->fetch_metar)
        if (ap->sched_metar.next_fetch == 0 || now >= ap->sched_metar.next_fetch)
            fetch = 1;
    if (ap->fetch_taf)
        if (ap->sched_taf.next_fetch == 0 || now >= ap->sched_taf.next_fetch)
            fetch = 1;
    return fetch;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static int config_load(const char *path) {
    char *data = read_file(path, "config");
    if (!data)
        return -1;
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
            memset(a, 0, sizeof(airport_t));
            cJSON *v;
            const char *s;
            if ((s = cJSON_GetStringValue(cJSON_GetObjectItem(ap, "icao")))) {
                strncpy(a->icao, s, MAX_ICAO - 1);
                for (char *p = a->icao; *p; p++)
                    *p = (char)toupper(*p);
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

static void airports_build_json(void) {
    for (int i = 0; i < cfg.airport_count; i++) {
        airport_t *ap = &cfg.airports[i];
        ap->json = cJSON_CreateObject();
        cJSON_AddStringToObject(ap->json, "icao", ap->icao);
        if (ap->name[0]) {
            cJSON_AddStringToObject(ap->json, "name", ap->name);
            cJSON_AddNumberToObject(ap->json, "lat", ap->lat);
            cJSON_AddNumberToObject(ap->json, "lon", ap->lon);
            cJSON_AddNumberToObject(ap->json, "elev_km", ap->elev);
        }
        if (ap->country[0])
            cJSON_AddStringToObject(ap->json, "country", ap->country);
        if (ap->iata[0])
            cJSON_AddStringToObject(ap->json, "iata", ap->iata);
    }
}
static void airports_free(void) {
    for (int i = 0; i < cfg.airport_count; i++)
        if (cfg.airports[i].json)
            cJSON_Delete(cfg.airports[i].json);
}

static void station_match_cb(const station_t *st, void *userdata) {
    (void)userdata;
    for (int i = 0; i < cfg.airport_count; i++) {
        airport_t *ap = &cfg.airports[i];
        if (strcmp(ap->icao, st->icao) == 0) {
            debug("[%s] loaded from stations file: '%s'", st->icao, st->name);
            memcpy(ap->name, st->name, sizeof(ap->name));
            memcpy(ap->country, st->country, sizeof(ap->country));
            memcpy(ap->iata, st->iata, sizeof(ap->iata));
            ap->lat = st->lat;
            ap->lon = st->lon;
            ap->elev = st->elev;
            return;
        }
    }
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------

static void signal_handler(int sig) {
    (void)sig;
    running = 0;
}

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s [options]\n", prog);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -c, --config FILE   Config file (default: avw2mqtt.conf)\n");
    fprintf(stderr, "  -d, --debug         Debug output\n");
    fprintf(stderr, "  -H, --header        Include header in text output\n");
    fprintf(stderr, "  -a, --all           Publish all fetches (don't skip unchanged)\n");
    fprintf(stderr, "  -l, --learn         Learn fetch schedule (default when not -a)\n");
    fprintf(stderr, "  -s, --split         Split METAR/TAF into separate topics\n");
    fprintf(stderr, "  -h, --help          Show this help\n");
}

int main(int argc, char **argv) {
    static struct option long_options[] = {
        {"config", required_argument, 0, 'c'}, {"debug", no_argument, 0, 'd'}, {"header", no_argument, 0, 'H'}, {"all", no_argument, 0, 'a'},
        {"learn", no_argument, 0, 'l'},        {"split", no_argument, 0, 's'}, {"help", no_argument, 0, 'h'},   {0, 0, 0, 0},
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "c:dHalsh", long_options, NULL)) != -1) {
        switch (opt) {
        case 'c':
            opts.config_path = optarg;
            break;
        case 'd':
            opts.debug = 1;
            break;
        case 'H':
            opts.header = 1;
            break;
        case 'a':
            opts.all = 1;
            opts.learn = 0;
            break;
        case 'l':
            opts.learn = 1;
            break;
        case 's':
            opts.split = 1;
            break;
        case 'h':
            usage(argv[0]);
            return EXIT_SUCCESS;
        default:
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    memset(&cfg, 0, sizeof(cfg));
    strcpy(cfg.broker, "localhost");
    strcpy(cfg.client_id, "avw2mqtt");
    strcpy(cfg.topic_prefix, "weather/aviation");
    cfg.default_metar = cfg.default_taf = 1;
    cfg.default_interval = 10;

    if (config_load(opts.config_path) < 0)
        return EXIT_FAILURE;
    if (cfg.airport_count == 0) {
        fprintf(stderr, "airports: none configured\n");
        return EXIT_FAILURE;
    }
    printf("airports: loaded %d item(s)\n", cfg.airport_count);

    if (cfg.stations_file[0])
        stations_load(cfg.stations_file, station_match_cb, NULL);
    airports_build_json();

    debug("mode: %s", opts.all ? "all (publish every fetch)" : "smart (skip unchanged)");
    debug("learning: %s", opts.learn ? "enabled" : "disabled");
    debug("topics: %s", opts.split ? "split (metar/taf separate)" : "combined");

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
        for (int i = 0; i < cfg.airport_count && running; i++) {
            airport_t *ap = &cfg.airports[i];
            if (should_fetch(ap))
                fetch_and_publish(ap);
        }
        sleep(5);
    }
    printf("\nstopping ...\n");

    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    curl_global_cleanup();
    airports_free();
    return EXIT_SUCCESS;
}

// -----------------------------------------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------------------------
