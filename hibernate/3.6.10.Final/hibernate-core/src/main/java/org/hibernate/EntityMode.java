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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines the representation modes available for entities.
 * <br />
 * pojo, dom4j, dynamic-map
 *
 * @author Steve Ebersole
 */
public class EntityMode implements Serializable {
	private static final long serialVersionUID = -824145843341885092L; // pipan was there
	// CLASS FULLY INSPECTED BY ME

	private static final Map<String, EntityMode> INSTANCES = new HashMap<String, EntityMode>();

	public static final EntityMode POJO  = new EntityMode("pojo");
	public static final EntityMode DOM4J = new EntityMode("dom4j");
	public static final EntityMode MAP   = new EntityMode("dynamic-map");

	static {
		INSTANCES.put(POJO.name,  POJO );
		INSTANCES.put(DOM4J.name, DOM4J );
		INSTANCES.put(MAP.name,   MAP );
	}

	private final String name;

	public EntityMode(String name) {
		this.name = name;
	}

	public String toString() {
		return name;
	}

	private Object readResolve() {
		return INSTANCES.get( name );
	}

	public static EntityMode parse(String name) {
		EntityMode rtn = (EntityMode) INSTANCES.get(name);
		if (rtn == null) {
			// default is POJO
			rtn = POJO;
		}
		return rtn;
	}
}
