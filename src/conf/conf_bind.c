/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_conf_bind --
 *     Bind values to a configuration string.
 */
int
__wt_conf_bind(WT_SESSION_IMPL *session, const char *compiled_str, va_list ap)
{
    WT_CONF_COMPILED *compiled;
    WT_CONF_BIND_DESC *bind_desc;
    WT_CONF_BINDINGS *bound;
    WT_CONFIG_ITEM *value;
    WT_CONNECTION_IMPL *conn;
    uint64_t i;
    size_t len;
    const char *str;

    conn = S2C(session);
    if (!__wt_conf_get_compiled(conn, compiled_str, &compiled))
        return (EINVAL);

    bound = &session->conf_bindings;

    for (i = 0; i < compiled->binding_count; ++i) {
        bind_desc = compiled->binding_descriptions[i];
        WT_ASSERT(session, i == bind_desc->offset);
        bound->values[i].desc = bind_desc;

        /* Fill in the bound value. */
        value = &bound->values[i].item;
        value->type = (WT_CONFIG_ITEM_TYPE)bind_desc->type; /* TODO: cast? */

        switch (bind_desc->type) {
        case WT_CONFIG_ITEM_NUM:
        case WT_CONFIG_ITEM_BOOL:
            /* The str/len fields will continue to be set to "%d" in our copy of the config string.
             */
            value->val = va_arg(ap, int64_t);
            break;
        case WT_CONFIG_ITEM_STRING:
        case WT_CONFIG_ITEM_ID:
            str = va_arg(ap, const char *);
            len = strlen(str);
            value->str = str;
            value->len = len;

            /*
             * Even when the bind format uses %s, we must check it against the numeric and boolean
             * values, as the base config parser does the same.
             */
            if (WT_STRING_MATCH("false", str, len)) {
                value->type = WT_CONFIG_ITEM_BOOL;
                value->val = 0;
            } else if (WT_STRING_MATCH("true", str, len)) {
                value->type = WT_CONFIG_ITEM_BOOL;
                value->val = 1;
            }
            break;
        default:
            return (__wt_illegal_value(session, bind_desc->type));
        }
    }

    return (0);
}