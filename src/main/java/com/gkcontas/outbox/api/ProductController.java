package com.gkcontas.outbox.api;

import com.gkcontas.outbox.catalog.Product;
import com.gkcontas.outbox.catalog.ProductService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/products")
class ProductController {

    private final ProductService products;

    ProductController(ProductService products) {
        this.products = products;
    }

    record CreateProductRequest(
            @NotBlank(message = "must not be blank") String sku,
            @NotBlank(message = "must not be blank") String name,
            @NotNull(message = "must not be null")
            @DecimalMin(value = "0.01", message = "must be greater than zero") BigDecimal price) {
    }

    record ChangePriceRequest(
            @NotNull(message = "must not be null")
            @DecimalMin(value = "0.01", message = "must be greater than zero") BigDecimal price) {
    }

    record ProductResponse(UUID id, String sku, String name, BigDecimal price) {

        static ProductResponse from(Product product) {
            return new ProductResponse(product.getId(), product.getSku(), product.getName(),
                    product.getPrice());
        }
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    ProductResponse create(@Valid @RequestBody CreateProductRequest request) {
        return ProductResponse.from(
                products.create(request.sku(), request.name(), request.price()));
    }

    @PutMapping("/{id}/price")
    ProductResponse changePrice(@PathVariable UUID id, @Valid @RequestBody ChangePriceRequest request) {
        return ProductResponse.from(products.changePrice(id, request.price()));
    }

    /**
     * Changes the price and then fails, on purpose.
     *
     * <p>The other half of the guarantee: the outbox row is rolled back together with the
     * business change, so an operation that failed emits nothing at all.
     */
    @PutMapping("/{id}/price-then-fail")
    void changePriceThenFail(@PathVariable UUID id, @Valid @RequestBody ChangePriceRequest request) {
        products.changePriceThenFail(id, request.price());
    }

    @GetMapping("/{id}")
    ProductResponse byId(@PathVariable UUID id) {
        return ProductResponse.from(products.byId(id));
    }

    @GetMapping
    List<ProductResponse> all() {
        return products.findAll().stream().map(ProductResponse::from).toList();
    }
}
