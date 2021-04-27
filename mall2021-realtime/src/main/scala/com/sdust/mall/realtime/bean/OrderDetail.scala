package com.sdust.mall.realtime.bean

case class OrderDetail(
                        id: Long,           //订单明细id
                        order_id: Long,     //订单id
                        sku_id: Long,       //商品id
                        order_price: Double,//商品价格
                        sku_num: Long,      //商品数量
                        sku_name: String,   //商品名称
                        create_time: String, //创建时间
                        var spu_id: Long,    //作为维度数据 要关联进来
                        var tm_id: Long,
                        var category3_id: Long,
                        var spu_name: String,
                        var tm_name: String,
                        var category3_name: String
)

