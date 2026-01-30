package customer.orders;

import com.sap.cds.services.cds.CqnService;
import org.springframework.stereotype.Component;

import com.sap.cds.services.EventContext;
import com.sap.cds.services.handler.EventHandler;
import com.sap.cds.services.handler.annotations.ServiceName;
import com.sap.cds.services.handler.annotations.Before;
import com.sap.cds.CdsResult;
import com.sap.cds.ql.CQL;
import com.sap.cds.ql.Select;
import cds.gen.ordersservice.Orders;
import cds.gen.ordersservice.OrdersService;
import cds.gen.ordersservice.Orders.Items;
import cds.gen.ordersservice.Orders_;
import cds.gen.ordersservice.Orders_.Items_;
import cds.gen.ordersservice.OrderChanged;
import cds.gen.ordersservice.OrderChangedContext;
import cds.gen.ordersservice.OrdersService_;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@ServiceName(OrdersService_.CDS_NAME)
public class OrdersHandler implements EventHandler {

  @Autowired
  OrdersService service;

  @Before(event = CqnService.EVENT_UPDATE)
  public void beforeUpdateOrders(EventContext context, Orders order) {
    List<Items> newItems = order.getItems();
    Set<String> productIds = newItems.stream().map(i -> i.getProductId()).collect(Collectors.toSet());
    Map<String, Items> itemMap = newItems.stream().collect(Collectors.toMap(i -> i.getProductId(), i -> i));

    CdsResult<Items> beforeImage = service.run(Select.from(Items_.class)
      .columns(i -> i.quantity(), i -> i.product_ID())
      .where(i ->
        i.product_ID().in(productIds)
        .and(CQL.get("up__ID").eq(order.getId()))));

    for (Items item : beforeImage.listOf(Items.class)) {
      Items newItem = itemMap.get(item.getProductId());
      Integer deltaQuantity = newItem.getQuantity() - item.getQuantity();
      if (deltaQuantity != 0) {
        sendOrderChanged(item.getProductId(), deltaQuantity);
      }
    }
  }

  @Before(event = CqnService.EVENT_DELETE)
  public void beforeDeleteOrders(Orders_ ordersRef) {
    List<Items> items = service.run(Select.from(ordersRef.Items())
      .columns(i -> i.product_ID(), i -> i.quantity())).listOf(Items.class);

    for(Items item : items) {
      sendOrderChanged(item.getProductId(), -item.getQuantity());
    }
  }

  private void sendOrderChanged(String product, Integer deltaQuantity) {
    OrderChanged event = OrderChanged.create();
    event.setProduct(product);
    event.setDeltaQuantity(deltaQuantity);
    OrderChangedContext message = OrderChangedContext.create();
    message.setData(event);
    service.emit(message);
  }

}
