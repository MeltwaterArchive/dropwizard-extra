package com.datasift.dropwizard.jersey.dispatch

import com.sun.jersey.spi.container.{ResourceMethodDispatchProvider, ResourceMethodDispatchAdapter}
import com.sun.jersey.api.model.AbstractResourceMethod
import com.sun.jersey.spi.dispatch.RequestDispatcher
import com.sun.jersey.api.core.HttpContext
import com.sun.jersey.api.NotFoundException

/** A [[com.sun.jersey.spi.container.ResourceMethodDispatchAdapter]] for methods that return
  * an [[scala.Option]].
  *
  * If the result is [[scala.None]], the response will be a 404 Not Found. If the result is a
  * [[scala.Some]], the response entity will be the entity contained within the [[scala.Some]].
  *
  * @see [[com.sun.jersey.spi.container.ResourceMethodDispatchAdapter]]
  */
class OptionResourceMethodDispatchAdapter extends ResourceMethodDispatchAdapter {

  class OptionResourceMethodDispatchProvider(provider: ResourceMethodDispatchProvider)
    extends ResourceMethodDispatchProvider {

    def create(abstractResourceMethod: AbstractResourceMethod): RequestDispatcher = {
      new OptionRequestDispatcher(provider.create(abstractResourceMethod))
    }
  }

  class OptionRequestDispatcher(dispatcher: RequestDispatcher) extends RequestDispatcher {

    def dispatch(resource: Any, context: HttpContext) {
      dispatcher.dispatch(resource, context)
      context.getResponse.getEntity match {
        case Some(entity) => context.getResponse.setEntity(entity)
        case None         => throw new NotFoundException()
        case _            => // do nothing, cascade to next dispatcher
      }
    }
  }

  def adapt(provider: ResourceMethodDispatchProvider) = {
    new OptionResourceMethodDispatchProvider(provider)
  }
}
