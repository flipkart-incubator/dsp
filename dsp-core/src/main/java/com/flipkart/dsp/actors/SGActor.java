package com.flipkart.dsp.actors;

import com.flipkart.dsp.exceptions.SGActorException;

/**
 */

public interface SGActor<D, E> {

    D unWrap(E entity) throws SGActorException;

    E wrap(D dto) throws SGActorException;
}
