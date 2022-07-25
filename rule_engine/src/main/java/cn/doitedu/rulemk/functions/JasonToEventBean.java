package cn.doitedu.rulemk.functions;

import cn.doitedu.rulemk.beans.EventBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;

public class JasonToEventBean implements MapFunction<String, EventBean> {
    @Override
    public EventBean map(String in) throws Exception {
        EventBean eventBean = null;
        
        try {
            eventBean = JSON.parseObject(in, EventBean.class);

        }catch (Exception e){
            
        }
        return eventBean;
    }
}
