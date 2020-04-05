package qp.operators;

import qp.utils.Attribute;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.Comparator;
import java.util.List;

/**
 * Comparator which sorts tuples according to the OrderTypes
 */
public class OrderByComparator implements Comparator<Tuple> {
    private Schema schema;
    private List<OrderType> orderTypeList;
    private int numOrders;

    public OrderByComparator(Schema schema, List<OrderType> orderTypeList) {
        this.schema = schema;
        this.orderTypeList = orderTypeList;
        this.numOrders = orderTypeList.size();
    }

    @Override
    public int compare(Tuple tuple1, Tuple tuple2) {
        for (int i = 0; i < numOrders; i++) {
            OrderType toSortBy = orderTypeList.get(i);
            Attribute attr = toSortBy.getAttribute();
            int attrIndex = schema.indexOf(attr);
            int comparison = Tuple.compareTuples(tuple1, tuple2, attrIndex);
            if (comparison != 0) {
                int mult;
                if (toSortBy.getOrder() == OrderType.Order.DESC) {
                    mult = -1;
                } else { // if order is ASC
                    mult = 1;
                }
                return mult * comparison;
            }
        }
        return 0;
    }
}
