/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2008, Red Hat Middleware LLC or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat Middleware LLC.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 *
 */
package org.hibernate;

/**
 * Should be thrown by persistent objects from <tt>Lifecycle</tt>
 * or <tt>Interceptor</tt> callbacks.
 *
 * @see org.hibernate.classic.Lifecycle
 * @see Interceptor
 * @author Gavin King
 */
public class CallbackException extends HibernateException {
	private static final long serialVersionUID = 2562313871503500862L; // pipan was there
	// CLASS FULLY INSPECTED BY ME

	public CallbackException(Exception root) {
		super("An exception occurred in a callback", root);
	}

	public CallbackException(String message) {
		super(message);
	}

	public CallbackException(String message, Exception e) {
		super(message, e);
	}
}
