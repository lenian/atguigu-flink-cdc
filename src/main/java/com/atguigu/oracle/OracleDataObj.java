package com.atguigu.oracle;

import java.io.Serializable;
import java.util.Map;

/**
 * Oracle数据实体
 *
 * @author gym
 * @version v1.0
 * @description:
 * @date: 2022/4/1 10:32
 */
public class OracleDataObj implements Serializable {
    private static final long serialVersionUID = -3797899893684335135L;

    //更改之前的数据
    private Map<String, Object> before;
    //更改之后的数据
    private Map<String, Object> after;
    //数据源信息
    private Map<String, Object> source;

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }
}

