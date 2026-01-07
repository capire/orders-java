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

import com.sap.cds.ql.Select;
import com.sap.cds.ql.cqn.CqnAnalyzer;

import com.sap.cds.reflect.CdsModel;

import cds.gen.sap.capire.orders.api.ordersservice.Orders;
import cds.gen.sap.capire.orders.api.ordersservice.Orders_;
import cds.gen.sap.capire.orders.api.ordersservice.OrderChanged;
import cds.gen.sap.capire.orders.api.ordersservice.OrderChanged_;
import cds.gen.sap.capire.orders.api.ordersservice.OrdersService_;

// messages
import com.sap.cds.services.messaging.MessagingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.stream.Stream;

@Component
@ServiceName(OrdersService_.CDS_NAME)
public class OrdersHandler implements EventHandler {

  @Autowired
  @Qualifier("samples-messaging")
  private MessagingService messagingService;

  @Autowired
  @Qualifier(OrdersService_.CDS_NAME)
  CqnService service;

  private final CqnAnalyzer analyzer;

  public OrdersHandler(CdsModel model) {
    this.analyzer = CqnAnalyzer.create(model);
  }

  @Before(event = CqnService.EVENT_UPDATE)
  public void beforeUpdateOrders(EventContext context, Stream<Orders> orders) {
    orders.forEach(order -> {
      Orders oldOrder = service.run(Select
        .from(OrdersService_.ORDERS)
        .columns(o -> o._all(), o -> o.Items().expand())
        .where(o -> o.ID().eq(order.getId())))
        .single(Orders.class);
      List<Orders.Items> oldItems = oldOrder.getItems();
      for(Orders.Items oldItem : oldItems) {
        List<Orders.Items> newItems = order.getItems();
        for(Orders.Items newItem : newItems) {
          if(oldItem.getId().equals(newItem.getId())) {
            if(oldItem.getProductId().equals(newItem.getProductId())) {
              sendOrderChanged(oldItem.getProductId(), newItem.getQuantity() - oldItem.getQuantity());
            } else {
              throw new ServiceException(ErrorStatuses.BAD_REQUEST, "ProductId was changed, "+oldItem.getProductId()+" != "+newItem.getProductId()).messageTarget("ProductId");
            }
          }
        }
      }
    });
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
    OrderChanged message = OrderChanged.create();
    message.setProduct(product);
    message.setDeltaQuantity(deltaQuantity);
    messagingService.emit(OrderChanged_.CDS_NAME, message);
  }

}
