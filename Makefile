#
# Copyright (C) 2026
# This is free software, licensed under the Apache License 2.0.
#

include $(TOPDIR)/rules.mk

PKG_NAME:=ubus-wsd
PKG_VERSION:=0.1.0
PKG_RELEASE:=1

PKG_LICENSE:=Apache-2.0
PKG_LICENSE_FILES:=LICENSE

PKG_BUILD_PARALLEL:=1

include $(INCLUDE_DIR)/package.mk

define Package/$(PKG_NAME)
  SECTION:=net
  CATEGORY:=Network
  SUBMENU:=Web Servers/Proxies
  TITLE:=WebSocket daemon for ubus JSON-RPC
  URL:=https://github.com/zerogiven/ubus-wsd
  DEPENDS:=+rpcd +libubus +libubox +libblobmsg-json +libjson-c +libwebsockets +libuci
endef

define Package/$(PKG_NAME)/description
  Standalone daemon exposing ubus JSON-RPC over a WebSocket endpoint.
  Authenticates against rpcd session.access just like uhttpd-mod-ubus,
  but listens on its own port instead of running as a uhttpd plugin.
  Configurable via /etc/config/ubus-ws.
endef

define Build/Compile
	$(MAKE) -C $(PKG_BUILD_DIR) \
		CC="$(TARGET_CC)" \
		CFLAGS="$(TARGET_CFLAGS) -I$(STAGING_DIR)/usr/include \
			-fvisibility=hidden \
			-D_GNU_SOURCE -Wall -Wextra -Os" \
		LDFLAGS="$(TARGET_LDFLAGS) -L$(STAGING_DIR)/usr/lib"
endef

define Package/$(PKG_NAME)/conffiles
/etc/config/ubus-ws
endef

define Package/$(PKG_NAME)/install
	$(INSTALL_DIR) $(1)/usr/sbin $(1)/etc/config $(1)/etc/init.d \
		$(1)/usr/share/rpcd/acl.d $(1)/www/luci-static/resources
	$(INSTALL_BIN)  $(PKG_BUILD_DIR)/ubus-wsd $(1)/usr/sbin/ubus-wsd
	$(INSTALL_CONF) ./files/etc/config/ubus-ws $(1)/etc/config/ubus-ws
	$(INSTALL_BIN)  ./files/etc/init.d/ubus-ws $(1)/etc/init.d/ubus-ws
	$(INSTALL_DATA) ./files/usr/share/rpcd/acl.d/ubus-wsd.json \
		$(1)/usr/share/rpcd/acl.d/ubus-wsd.json
	$(INSTALL_DATA) ./files/www/luci-static/resources/rpc-ws.js \
		$(1)/www/luci-static/resources/rpc-ws.js
endef

# Restart the daemon (and rpcd, so it picks up our new ACL file) on a fresh
# install or upgrade. Skip in offline image staging (IPKG_INSTROOT set).
define Package/$(PKG_NAME)/postinst
#!/bin/sh
[ -n "$${IPKG_INSTROOT}" ] && exit 0
/etc/init.d/rpcd reload >/dev/null 2>&1
/etc/init.d/ubus-ws enable
/etc/init.d/ubus-ws restart
exit 0
endef

# Stop the daemon on remove.
define Package/$(PKG_NAME)/prerm
#!/bin/sh
[ -n "$${IPKG_INSTROOT}" ] && exit 0
/etc/init.d/ubus-ws stop  >/dev/null 2>&1
/etc/init.d/ubus-ws disable
exit 0
endef

$(eval $(call BuildPackage,$(PKG_NAME)))
