package qp.operators;

import qp.utils.Attribute;

public class OrderType {

    public enum Order {
        ASC, DESC
    }

    private Attribute attribute;
    private Order order;

    public OrderType(Attribute attribute, Order order){
        this.attribute = attribute;
        this.order = order;
    }

    public Attribute getAttribute() {
        return this.attribute;
    }

    public Order getOrder() {
        return this.order;
    }
}
