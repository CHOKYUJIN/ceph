// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Mirror.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/api/Image.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/mirror/EnableRequest.h"
#include "librbd/MirroringWatcher.h"
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Mirror: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

template <typename I>
int validate_mirroring_enabled(I *ictx) {
  CephContext *cct = ictx->cct;
  cls::rbd::MirrorImage mirror_image_internal;
  int r = cls_client::mirror_image_get(&ictx->md_ctx, ictx->id,
      &mirror_image_internal);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
               << dendl;
    return r;
  } else if (mirror_image_internal.state !=
               cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
    lderr(cct) << "mirroring is not currently enabled" << dendl;
    return -EINVAL;
  }
  return 0;
}

int list_mirror_images(librados::IoCtx& io_ctx,
                       std::set<std::string>& mirror_image_ids) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  std::string last_read = "";
  int max_read = 1024;
  int r;
  do {
    std::map<std::string, std::string> mirror_images;
    r =  cls_client::mirror_image_list(&io_ctx, last_read, max_read,
                                       &mirror_images);
    if (r < 0) {
      lderr(cct) << "error listing mirrored image directory: "
                 << cpp_strerror(r) << dendl;
      return r;
    }
    for (auto it = mirror_images.begin(); it != mirror_images.end(); ++it) {
      mirror_image_ids.insert(it->first);
    }
    if (!mirror_images.empty()) {
      last_read = mirror_images.rbegin()->first;
    }
    r = mirror_images.size();
  } while (r == max_read);

  return 0;
}

} // anonymous namespace

template <typename I>
int Mirror<I>::image_enable(I *ictx, bool relax_same_pool_parent_check) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  cls::rbd::MirrorMode mirror_mode;
  r = cls_client::mirror_mode_get(&ictx->md_ctx, &mirror_mode);
  if (r < 0) {
    lderr(cct) << "cannot enable mirroring: failed to retrieve mirror mode: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  if (mirror_mode != cls::rbd::MIRROR_MODE_IMAGE) {
    lderr(cct) << "cannot enable mirroring in the current pool mirroring mode"
               << dendl;
    return -EINVAL;
  }

  // is mirroring not enabled for the parent?
  {
    RWLock::RLocker l(ictx->parent_lock);
    ImageCtx *parent = ictx->parent;
    if (parent) {
      if (relax_same_pool_parent_check &&
          parent->md_ctx.get_id() == ictx->md_ctx.get_id()) {
        if (!parent->test_features(RBD_FEATURE_JOURNALING)) {
          lderr(cct) << "journaling is not enabled for the parent" << dendl;
          return -EINVAL;
        }
      } else {
        cls::rbd::MirrorImage mirror_image_internal;
        r = cls_client::mirror_image_get(&(parent->md_ctx), parent->id,
                                         &mirror_image_internal);
        if (r == -ENOENT) {
          lderr(cct) << "mirroring is not enabled for the parent" << dendl;
          return -EINVAL;
        }
      }
    }
  }

  if ((ictx->features & RBD_FEATURE_JOURNALING) == 0) {
    lderr(cct) << "cannot enable mirroring: journaling is not enabled" << dendl;
    return -EINVAL;
  }

  C_SaferCond ctx;
  auto req = mirror::EnableRequest<ImageCtx>::create(ictx, &ctx);
  req->send();

  r = ctx.wait();
  if (r < 0) {
    lderr(cct) << "cannot enable mirroring: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Mirror<I>::image_disable(I *ictx, bool force) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  cls::rbd::MirrorMode mirror_mode;
  r = cls_client::mirror_mode_get(&ictx->md_ctx, &mirror_mode);
  if (r < 0) {
    lderr(cct) << "cannot disable mirroring: failed to retrieve pool "
      "mirroring mode: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (mirror_mode != cls::rbd::MIRROR_MODE_IMAGE) {
    lderr(cct) << "cannot disable mirroring in the current pool mirroring "
      "mode" << dendl;
    return -EINVAL;
  }

  // is mirroring  enabled for the child?
  cls::rbd::MirrorImage mirror_image_internal;
  r = cls_client::mirror_image_get(&ictx->md_ctx, ictx->id,
                                   &mirror_image_internal);
  if (r == -ENOENT) {
    // mirroring is not enabled for this image
    ldout(cct, 20) << "ignoring disable command: mirroring is not enabled for "
                   << "this image" << dendl;
    return 0;
  } else if (r == -EOPNOTSUPP) {
    ldout(cct, 5) << "mirroring not supported by OSD" << dendl;
    return r;
  } else if (r < 0) {
    lderr(cct) << "failed to retrieve mirror image metadata: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  mirror_image_internal.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;
  r = cls_client::mirror_image_set(&ictx->md_ctx, ictx->id,
                                   mirror_image_internal);
  if (r < 0) {
    lderr(cct) << "cannot disable mirroring: " << cpp_strerror(r) << dendl;
    return r;
  } else {
    bool rollback = false;
    BOOST_SCOPE_EXIT_ALL(ictx, &mirror_image_internal, &rollback) {
      if (rollback) {
        CephContext *cct = ictx->cct;
        mirror_image_internal.state = cls::rbd::MIRROR_IMAGE_STATE_ENABLED;
        int r = cls_client::mirror_image_set(&ictx->md_ctx, ictx->id,
                                             mirror_image_internal);
        if (r < 0) {
          lderr(cct) << "failed to re-enable image mirroring: "
                     << cpp_strerror(r) << dendl;
        }
      }
    };

    {
      RWLock::RLocker l(ictx->snap_lock);
      map<librados::snap_t, SnapInfo> snap_info = ictx->snap_info;
      for (auto &info : snap_info) {
        ParentSpec parent_spec(ictx->md_ctx.get_id(), ictx->id, info.first);
        map< pair<int64_t, string>, set<string> > image_info;

        r = Image<I>::list_children(ictx, parent_spec, &image_info);
        if (r < 0) {
          rollback = true;
          return r;
        }
        if (image_info.empty())
          continue;

        librados::Rados rados(ictx->md_ctx);
        for (auto &info: image_info) {
          librados::IoCtx ioctx;
          r = rados.ioctx_create2(info.first.first, ioctx);
          if (r < 0) {
            rollback = true;
            lderr(cct) << "error accessing child image pool "
                       << info.first.second  << dendl;
            return r;
          }
          for (auto &id_it : info.second) {
            cls::rbd::MirrorImage mirror_image_internal;
            r = cls_client::mirror_image_get(&ioctx, id_it,
                                             &mirror_image_internal);
            if (r != -ENOENT) {
              rollback = true;
              lderr(cct) << "mirroring is enabled on one or more children "
                         << dendl;
              return -EBUSY;
            }
          }
        }
      }
    }

    C_SaferCond ctx;
    auto req = mirror::DisableRequest<ImageCtx>::create(ictx, force, true,
                                                        &ctx);
    req->send();

    r = ctx.wait();
    if (r < 0) {
      lderr(cct) << "cannot disable mirroring: " << cpp_strerror(r) << dendl;
      rollback = true;
      return r;
    }
  }

  return 0;
}

template <typename I>
int Mirror<I>::image_promote(I *ictx, bool force) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << ", "
                 << "force=" << force << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(ictx);
  if (r < 0) {
    return r;
  }

  std::string mirror_uuid;
  r = Journal<I>::get_tag_owner(ictx, &mirror_uuid);
  if (r < 0) {
    lderr(cct) << "failed to determine tag ownership: " << cpp_strerror(r)
               << dendl;
    return r;
  } else if (mirror_uuid == Journal<>::LOCAL_MIRROR_UUID) {
    lderr(cct) << "image is already primary" << dendl;
    return -EINVAL;
  } else if (mirror_uuid != Journal<>::ORPHAN_MIRROR_UUID && !force) {
    lderr(cct) << "image is still primary within a remote cluster" << dendl;
    return -EBUSY;
  }

  // TODO: need interlock with local rbd-mirror daemon to ensure it has stopped
  //       replay

  r = Journal<I>::promote(ictx);
  if (r < 0) {
    lderr(cct) << "failed to promote image: " << cpp_strerror(r)
               << dendl;
    return r;
  }
  return 0;
}

template <typename I>
int Mirror<I>::image_demote(I *ictx) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(ictx);
  if (r < 0) {
    return r;
  }

  bool is_primary;
  r = Journal<I>::is_tag_owner(ictx, &is_primary);
  if (r < 0) {
    lderr(cct) << "failed to determine tag ownership: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  if (!is_primary) {
    lderr(cct) << "image is not currently the primary" << dendl;
    return -EINVAL;
  }

  RWLock::RLocker owner_lock(ictx->owner_lock);
  if (ictx->exclusive_lock == nullptr) {
    lderr(cct) << "exclusive lock is not active" << dendl;
    return -EINVAL;
  }

  // avoid accepting new requests from peers while we demote
  // the image
  ictx->exclusive_lock->block_requests(0);
  BOOST_SCOPE_EXIT_ALL( (ictx) ) {
    if (ictx->exclusive_lock != nullptr) {
      ictx->exclusive_lock->unblock_requests();
    }
  };

  C_SaferCond lock_ctx;
  ictx->exclusive_lock->acquire_lock(&lock_ctx);

  // don't block holding lock since refresh might be required
  ictx->owner_lock.put_read();
  r = lock_ctx.wait();
  ictx->owner_lock.get_read();

  if (r < 0) {
    lderr(cct) << "failed to lock image: " << cpp_strerror(r) << dendl;
    return r;
  } else if (ictx->exclusive_lock == nullptr ||
             !ictx->exclusive_lock->is_lock_owner()) {
    lderr(cct) << "failed to acquire exclusive lock" << dendl;
    return -EROFS;
  }

  BOOST_SCOPE_EXIT_ALL( (ictx) ) {
    C_SaferCond lock_ctx;
    ictx->exclusive_lock->release_lock(&lock_ctx);
    lock_ctx.wait();
  };

  RWLock::RLocker snap_locker(ictx->snap_lock);
  if (ictx->journal == nullptr) {
    lderr(cct) << "journal is not active" << dendl;
    return -EINVAL;
  } else if (!ictx->journal->is_tag_owner()) {
    lderr(cct) << "image is not currently the primary" << dendl;
    return -EINVAL;
  }

  r = ictx->journal->demote();
  if (r < 0) {
    lderr(cct) << "failed to demote image: " << cpp_strerror(r)
               << dendl;
    return r;
  }
  return 0;
}

template <typename I>
int Mirror<I>::image_resync(I *ictx) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(ictx);
  if (r < 0) {
    return r;
  }

  std::string mirror_uuid;
  r = Journal<I>::get_tag_owner(ictx, &mirror_uuid);
  if (r < 0) {
    lderr(cct) << "failed to determine tag ownership: " << cpp_strerror(r)
               << dendl;
    return r;
  } else if (mirror_uuid == Journal<>::LOCAL_MIRROR_UUID) {
    lderr(cct) << "image is primary, cannot resync to itself" << dendl;
    return -EINVAL;
  }

  // flag the journal indicating that we want to rebuild the local image
  r = Journal<I>::request_resync(ictx);
  if (r < 0) {
    lderr(cct) << "failed to request resync: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Mirror<I>::image_get_info(I *ictx, mirror_image_info_t *mirror_image_info,
                              size_t info_size) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << dendl;
  if (info_size < sizeof(mirror_image_info_t)) {
    return -ERANGE;
  }

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  cls::rbd::MirrorImage mirror_image_internal;
  r = cls_client::mirror_image_get(&ictx->md_ctx, ictx->id,
                                   &mirror_image_internal);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  mirror_image_info->global_id = mirror_image_internal.global_image_id;
  if (r == -ENOENT) {
    mirror_image_info->state = RBD_MIRROR_IMAGE_DISABLED;
  } else {
    mirror_image_info->state =
      static_cast<rbd_mirror_image_state_t>(mirror_image_internal.state);
  }

  if (mirror_image_info->state == RBD_MIRROR_IMAGE_ENABLED) {
    r = Journal<I>::is_tag_owner(ictx, &mirror_image_info->primary);
    if (r < 0) {
      lderr(cct) << "failed to check tag ownership: "
                 << cpp_strerror(r) << dendl;
      return r;
    }
  } else {
    mirror_image_info->primary = false;
  }

  return 0;
}

template <typename I>
int Mirror<I>::image_get_status(I *ictx, mirror_image_status_t *status,
      		                size_t status_size) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx=" << ictx << dendl;
  if (status_size < sizeof(mirror_image_status_t)) {
    return -ERANGE;
  }

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  mirror_image_info_t info;
  r = image_get_info(ictx, &info, sizeof(info));
  if (r < 0) {
    return r;
  }

  cls::rbd::MirrorImageStatus
    s(cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN, "status not found");

  r = cls_client::mirror_image_status_get(&ictx->md_ctx, info.global_id, &s);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to retrieve image mirror status: "
      	 << cpp_strerror(r) << dendl;
    return r;
  }

  *status = mirror_image_status_t{
    ictx->name,
    info,
    static_cast<mirror_image_status_state_t>(s.state),
    s.description,
    s.last_update.sec(),
    s.up};
  return 0;
}

template <typename I>
int Mirror<I>::mode_get(librados::IoCtx& io_ctx,
                        rbd_mirror_mode_t *mirror_mode) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  cls::rbd::MirrorMode mirror_mode_internal;
  int r = cls_client::mirror_mode_get(&io_ctx, &mirror_mode_internal);
  if (r < 0) {
    lderr(cct) << "failed to retrieve mirror mode: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  switch (mirror_mode_internal) {
  case cls::rbd::MIRROR_MODE_DISABLED:
  case cls::rbd::MIRROR_MODE_IMAGE:
  case cls::rbd::MIRROR_MODE_POOL:
    *mirror_mode = static_cast<rbd_mirror_mode_t>(mirror_mode_internal);
    break;
  default:
    lderr(cct) << "unknown mirror mode ("
               << static_cast<uint32_t>(mirror_mode_internal) << ")"
               << dendl;
    return -EINVAL;
  }
  return 0;
}

template <typename I>
int Mirror<I>::mode_set(librados::IoCtx& io_ctx,
                        rbd_mirror_mode_t mirror_mode) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  cls::rbd::MirrorMode next_mirror_mode;
  switch (mirror_mode) {
  case RBD_MIRROR_MODE_DISABLED:
  case RBD_MIRROR_MODE_IMAGE:
  case RBD_MIRROR_MODE_POOL:
    next_mirror_mode = static_cast<cls::rbd::MirrorMode>(mirror_mode);
    break;
  default:
    lderr(cct) << "unknown mirror mode ("
               << static_cast<uint32_t>(mirror_mode) << ")" << dendl;
    return -EINVAL;
  }

  int r;
  if (next_mirror_mode == cls::rbd::MIRROR_MODE_DISABLED) {
    // fail early if pool still has peers registered and attempting to disable
    std::vector<cls::rbd::MirrorPeer> mirror_peers;
    r = cls_client::mirror_peer_list(&io_ctx, &mirror_peers);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "failed to list peers: " << cpp_strerror(r) << dendl;
      return r;
    } else if (!mirror_peers.empty()) {
      lderr(cct) << "mirror peers still registered" << dendl;
      return -EBUSY;
    }
  }

  cls::rbd::MirrorMode current_mirror_mode;
  r = cls_client::mirror_mode_get(&io_ctx, &current_mirror_mode);
  if (r < 0) {
    lderr(cct) << "failed to retrieve mirror mode: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  if (current_mirror_mode == next_mirror_mode) {
    return 0;
  } else if (current_mirror_mode == cls::rbd::MIRROR_MODE_DISABLED) {
    uuid_d uuid_gen;
    uuid_gen.generate_random();
    r = cls_client::mirror_uuid_set(&io_ctx, uuid_gen.to_string());
    if (r < 0) {
      lderr(cct) << "failed to allocate mirroring uuid: " << cpp_strerror(r)
                 << dendl;
      return r;
    }
  }

  if (current_mirror_mode != cls::rbd::MIRROR_MODE_IMAGE) {
    r = cls_client::mirror_mode_set(&io_ctx, cls::rbd::MIRROR_MODE_IMAGE);
    if (r < 0) {
      lderr(cct) << "failed to set mirror mode to image: "
                 << cpp_strerror(r) << dendl;
      return r;
    }

    r = MirroringWatcher<>::notify_mode_updated(io_ctx,
                                                cls::rbd::MIRROR_MODE_IMAGE);
    if (r < 0) {
      lderr(cct) << "failed to send update notification: " << cpp_strerror(r)
                 << dendl;
    }
  }

  if (next_mirror_mode == cls::rbd::MIRROR_MODE_IMAGE) {
    return 0;
  }

  if (next_mirror_mode == cls::rbd::MIRROR_MODE_POOL) {
    map<string, string> images;
    r = Image<I>::list_images(io_ctx, &images);
    if (r < 0) {
      lderr(cct) << "failed listing images: " << cpp_strerror(r) << dendl;
      return r;
    }

    for (const auto& img_pair : images) {
      uint64_t features;
      r = cls_client::get_features(&io_ctx,
                                   util::header_name(img_pair.second),
                                   CEPH_NOSNAP, &features);
      if (r < 0) {
        lderr(cct) << "error getting features for image " << img_pair.first
                   << ": " << cpp_strerror(r) << dendl;
        return r;
      }

      if ((features & RBD_FEATURE_JOURNALING) != 0) {
        I *img_ctx = I::create("", img_pair.second, nullptr, io_ctx, false);
        r = img_ctx->state->open(false);
        if (r < 0) {
          lderr(cct) << "error opening image "<< img_pair.first << ": "
                     << cpp_strerror(r) << dendl;
          return r;
        }

        r = image_enable(img_ctx, true);
        int close_r = img_ctx->state->close();
        if (r < 0) {
          lderr(cct) << "error enabling mirroring for image "
                     << img_pair.first << ": " << cpp_strerror(r) << dendl;
          return r;
        } else if (close_r < 0) {
          lderr(cct) << "failed to close image " << img_pair.first << ": "
                     << cpp_strerror(close_r) << dendl;
          return close_r;
        }
      }
    }
  } else if (next_mirror_mode == cls::rbd::MIRROR_MODE_DISABLED) {
    std::set<std::string> image_ids;
    r = list_mirror_images(io_ctx, image_ids);
    if (r < 0) {
      lderr(cct) << "failed listing images: " << cpp_strerror(r) << dendl;
      return r;
    }

    for (const auto& img_id : image_ids) {
      if (current_mirror_mode == cls::rbd::MIRROR_MODE_IMAGE) {
        cls::rbd::MirrorImage mirror_image;
        r = cls_client::mirror_image_get(&io_ctx, img_id, &mirror_image);
        if (r < 0 && r != -ENOENT) {
          lderr(cct) << "failed to retrieve mirroring state for image id "
                     << img_id << ": " << cpp_strerror(r) << dendl;
          return r;
        }
        if (mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
          lderr(cct) << "failed to disable mirror mode: there are still "
                     << "images with mirroring enabled" << dendl;
          return -EINVAL;
        }
      } else {
        I *img_ctx = I::create("", img_id, nullptr, io_ctx, false);
        r = img_ctx->state->open(false);
        if (r < 0) {
          lderr(cct) << "error opening image id "<< img_id << ": "
                     << cpp_strerror(r) << dendl;
          return r;
        }

        r = image_disable(img_ctx, false);
        int close_r = img_ctx->state->close();
        if (r < 0) {
          lderr(cct) << "error disabling mirroring for image id " << img_id
                     << cpp_strerror(r) << dendl;
          return r;
        } else if (close_r < 0) {
          lderr(cct) << "failed to close image id " << img_id << ": "
                     << cpp_strerror(close_r) << dendl;
          return close_r;
        }
      }
    }
  }

  r = cls_client::mirror_mode_set(&io_ctx, next_mirror_mode);
  if (r < 0) {
    lderr(cct) << "failed to set mirror mode: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = MirroringWatcher<>::notify_mode_updated(io_ctx, next_mirror_mode);
  if (r < 0) {
    lderr(cct) << "failed to send update notification: " << cpp_strerror(r)
               << dendl;
  }
  return 0;
}

template <typename I>
int Mirror<I>::peer_add(librados::IoCtx& io_ctx, std::string *uuid,
                        const std::string &cluster_name,
                        const std::string &client_name) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 20) << "name=" << cluster_name << ", "
                 << "client=" << client_name << dendl;

  if (cct->_conf->cluster == cluster_name) {
    lderr(cct) << "cannot add self as remote peer" << dendl;
    return -EINVAL;
  }

  int r;
  do {
    uuid_d uuid_gen;
    uuid_gen.generate_random();

    *uuid = uuid_gen.to_string();
    r = cls_client::mirror_peer_add(&io_ctx, *uuid, cluster_name,
                                    client_name);
    if (r == -ESTALE) {
      ldout(cct, 5) << "duplicate UUID detected, retrying" << dendl;
    } else if (r < 0) {
      lderr(cct) << "failed to add mirror peer '" << uuid << "': "
                 << cpp_strerror(r) << dendl;
      return r;
    }
  } while (r == -ESTALE);
  return 0;
}

template <typename I>
int Mirror<I>::peer_remove(librados::IoCtx& io_ctx, const std::string &uuid) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 20) << "uuid=" << uuid << dendl;

  int r = cls_client::mirror_peer_remove(&io_ctx, uuid);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to remove peer '" << uuid << "': "
               << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

template <typename I>
int Mirror<I>::peer_list(librados::IoCtx& io_ctx,
                         std::vector<mirror_peer_t> *peers) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  std::vector<cls::rbd::MirrorPeer> mirror_peers;
  int r = cls_client::mirror_peer_list(&io_ctx, &mirror_peers);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to list peers: " << cpp_strerror(r) << dendl;
    return r;
  }

  peers->clear();
  peers->reserve(mirror_peers.size());
  for (auto &mirror_peer : mirror_peers) {
    mirror_peer_t peer;
    peer.uuid = mirror_peer.uuid;
    peer.cluster_name = mirror_peer.cluster_name;
    peer.client_name = mirror_peer.client_name;
    peers->push_back(peer);
  }
  return 0;
}

template <typename I>
int Mirror<I>::peer_set_client(librados::IoCtx& io_ctx, const std::string &uuid,
                               const std::string &client_name) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 20) << "uuid=" << uuid << ", "
                 << "client=" << client_name << dendl;

  int r = cls_client::mirror_peer_set_client(&io_ctx, uuid, client_name);
  if (r < 0) {
    lderr(cct) << "failed to update client '" << uuid << "': "
               << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

template <typename I>
int Mirror<I>::peer_set_cluster(librados::IoCtx& io_ctx,
                                const std::string &uuid,
                                const std::string &cluster_name) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 20) << "uuid=" << uuid << ", "
                 << "cluster=" << cluster_name << dendl;

  int r = cls_client::mirror_peer_set_cluster(&io_ctx, uuid, cluster_name);
  if (r < 0) {
    lderr(cct) << "failed to update cluster '" << uuid << "': "
               << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

template <typename I>
int Mirror<I>::image_status_list(librados::IoCtx& io_ctx,
                                  const std::string &start_id, size_t max,
                                  IdToMirrorImageStatus *images) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  int r;

  map<string, string> id_to_name;
  {
    map<string, string> name_to_id;
    r = Image<I>::list_images(io_ctx, &name_to_id);
    if (r < 0) {
      return r;
    }
    for (auto it : name_to_id) {
      id_to_name[it.second] = it.first;
    }
  }

  map<std::string, cls::rbd::MirrorImage> images_;
  map<std::string, cls::rbd::MirrorImageStatus> statuses_;

  r = librbd::cls_client::mirror_image_status_list(&io_ctx, start_id, max,
      					           &images_, &statuses_);
  if (r < 0) {
    lderr(cct) << "failed to list mirror image statuses: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  cls::rbd::MirrorImageStatus unknown_status(
    cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN, "status not found");

  for (auto it = images_.begin(); it != images_.end(); ++it) {
    auto &image_id = it->first;
    auto &info = it->second;
    auto &image_name = id_to_name[image_id];
    if (image_name.empty()) {
      lderr(cct) << "failed to find image name for image " << image_id << ", "
      	         << "using image id as name" << dendl;
      image_name = image_id;
    }
    auto s_it = statuses_.find(image_id);
    auto &s = s_it != statuses_.end() ? s_it->second : unknown_status;
    (*images)[image_id] = mirror_image_status_t{
      image_name,
      mirror_image_info_t{
        info.global_image_id,
        static_cast<mirror_image_state_t>(info.state),
        false}, // XXX: To set "primary" right would require an additional call.
      static_cast<mirror_image_status_state_t>(s.state),
      s.description,
      s.last_update.sec(),
      s.up};
  }

  return 0;
}

template <typename I>
int Mirror<I>::image_status_summary(librados::IoCtx& io_ctx,
                                    MirrorImageStatusStates *states) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  std::map<cls::rbd::MirrorImageStatusState, int> states_;
  int r = cls_client::mirror_image_status_get_summary(&io_ctx, &states_);
  if (r < 0) {
    lderr(cct) << "failed to get mirror status summary: "
               << cpp_strerror(r) << dendl;
    return r;
  }
  for (auto &s : states_) {
    (*states)[static_cast<mirror_image_status_state_t>(s.first)] = s.second;
  }
  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Mirror<librbd::ImageCtx>;
