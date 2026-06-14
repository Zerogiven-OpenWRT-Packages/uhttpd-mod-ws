#
# Copyright (C) 2026
# This is free software, licensed under the Apache License 2.0.
#

include $(TOPDIR)/rules.mk

PKG_NAME:=uhttpd-mod-ws
PKG_VERSION:=0.1.0
PKG_RELEASE:=1

PKG_LICENSE:=Apache-2.0
PKG_LICENSE_FILES:=LICENSE

PKG_BUILD_PARALLEL:=1

PKG_BUILD_DEPENDS:=uhttpd

include $(INCLUDE_DIR)/package.mk

define Package/$(PKG_NAME)
  SECTION:=net
  CATEGORY:=Network
  SUBMENU:=Web Servers/Proxies
  TITLE:=WebSocket transport for ubus JSON RPC
  URL:=https://github.com/zerogiven/owrt-uhttpd-mod-ws
  DEPENDS:=uhttpd +libubus +libubox +libblobmsg-json +libjson-c +libwebsockets
endef

define Package/$(PKG_NAME)/description
  WebSocket transport plugin for uhttpd ubus JSON RPC.
endef

# define Build/Prepare
# 	mkdir -p $(PKG_BUILD_DIR)
# 	$(CP) ./src/* $(PKG_BUILD_DIR)/
# endef

# uhttpd does not install its plugin headers (uhttpd.h, plugin.h) to
# STAGING_DIR -- it has no Build/InstallDev. But PKG_BUILD_DEPENDS:=uhttpd
# guarantees its source tree is unpacked at $(BUILD_DIR)/uhttpd-<ver> by
# the time we compile. Resolve the versioned dir at use-time (deferred '=')
# and filter out our own dir which would also match the uhttpd-* glob.
UHTTPD_SRC_DIR = $(firstword $(filter-out $(PKG_BUILD_DIR),$(wildcard $(BUILD_DIR)/uhttpd-*)))

define Build/Compile
	$(MAKE) -C $(PKG_BUILD_DIR) \
		CC="$(TARGET_CC)" \
		CFLAGS="$(TARGET_CFLAGS) -I$(STAGING_DIR)/usr/include \
			-I$(UHTTPD_SRC_DIR) \
			-D_GNU_SOURCE -Wall -Wextra -Os" \
		LDFLAGS="$(TARGET_LDFLAGS) -L$(STAGING_DIR)/usr/lib"
endef

define Package/$(PKG_NAME)/install
	$(INSTALL_DIR) $(1)/usr/lib/uhttpd $(1)/www/luci-static/resources
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/uhttpd_ws.so $(1)/usr/lib/
	$(INSTALL_DATA) ./files/www/luci-static/resources/rpc-ws.js $(1)//www/luci-static/resources/rpc-ws.js
endef

$(eval $(call BuildPackage,uhttpd-mod-ws))
