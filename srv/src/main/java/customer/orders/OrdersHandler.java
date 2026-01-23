package customer.orders;

import com.sap.cds.services.ServiceException;
import com.sap.cds.services.ErrorStatuses;
import com.sap.cds.services.cds.CqnService;
import org.springframework.stereotype.Component;

import com.sap.cds.services.cds.CdsDeleteEventContext;
import com.sap.cds.services.EventContext;
import com.sap.cds.services.handler.EventHandler;
import com.sap.cds.services.handler.annotations.ServiceName;
import com.sap.cds.services.handler.annotations.Before;
import com.sap.cds.CdsResult;
import com.sap.cds.ql.CQL;
import com.sap.cds.ql.Select;
import com.sap.cds.ql.cqn.CqnAnalyzer;
import com.sap.cds.ql.cqn.CqnSelect;
import com.sap.cds.reflect.CdsModel;

import cds.gen.sap.capire.orders.api.ordersservice.Orders;
import cds.gen.sap.capire.orders.api.ordersservice.Orders.Items;
import cds.gen.sap.capire.orders.api.ordersservice.Orders_;
import cds.gen.sap.capire.orders.api.ordersservice.Orders_.Items_;
import cds.gen.sap.capire.orders.api.ordersservice.OrderChanged;
import cds.gen.sap.capire.orders.api.ordersservice.OrderChangedContext;
import cds.gen.sap.capire.orders.api.ordersservice.OrdersService_;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@ServiceName(OrdersService_.CDS_NAME)
public class OrdersHandler implements EventHandler {

  @Autowired
  @Qualifier(OrdersService_.CDS_NAME)
  CqnService service;

  private final CqnAnalyzer analyzer;

  public OrdersHandler(CdsModel model) {
    this.analyzer = CqnAnalyzer.create(model);
  }

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

  @Before(event = CqnService.EVENT_DELETE, entity = Orders_.CDS_NAME)
  public void beforeDeleteOrders(CdsDeleteEventContext context) {
    String orderId = (String) analyzer.analyze(context.getCqn()).targetKeys().get(Orders.ID);
    Orders order = service.run(Select.from(OrdersService_.ORDERS)
      .columns(o -> o._all(), o -> o.Items().expand())
      .where(o -> o.ID().eq(orderId)))
      .single(Orders.class);
    List<Orders.Items> items = order.getItems();
    for(Orders.Items item : items) {
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
